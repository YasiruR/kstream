/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package librd

import (
	"context"
	"fmt"
	"sync"
	"time"

	librdKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/pkg/errors"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics/v2"
)

const (
	PartitionerRandom                  kafka.PartitionerType = `random`
	PartitionerCRC32                   kafka.PartitionerType = `consistent`
	PartitionerCRC32Random             kafka.PartitionerType = `consistent_random`
	PartitionerConsistentMurmur2       kafka.PartitionerType = `murmur2`
	PartitionerConsistentMurmur2Random kafka.PartitionerType = `murmur2_random`
	PartitionerConsistentFNV1a         kafka.PartitionerType = `fnv1a`
	PartitionerConsistentFNV1aRandom   kafka.PartitionerType = `fnv1a_random`
)

type DeliveryReport struct {
	librdReport *librdKafka.Message
}

func (d DeliveryReport) TopicPartition() kafka.TopicPartition {
	return kafka.TopicPartition{
		Topic:     *d.librdReport.TopicPartition.Topic,
		Partition: d.librdReport.TopicPartition.Partition,
	}
}

func (d DeliveryReport) Offset() kafka.Offset {
	return kafka.Offset(d.librdReport.TopicPartition.Offset)
}

func (d DeliveryReport) Error() error {
	return d.librdReport.TopicPartition.Error
}

func (d DeliveryReport) Delivered() bool {
	return d.librdReport.TopicPartition.Error == nil
}

type Producer struct {
	config       *ProducerConfig
	baseProducer *librdKafka.Producer

	metrics struct {
		produceLatency metrics.Observer
		transactions   struct {
			initLatency        metrics.Observer
			sendOffsetsLatency metrics.Observer
			commitLatency      metrics.Observer
			abortLatency       metrics.Observer
		}
		produceErrors metrics.Counter
	}

	logsChannelClosed chan struct{}
	eventsChanClosed  chan struct{}

	partitionCounts *sync.Map
}

type producerProvider struct {
	config *ProducerConfig
}

// NewProducerProvider creates a new instance of kafka.ProducerProvider initialized with the provided ProducerConfig.
//
// The returned ProducerProvider can be used to create producer builders that will generate Kafka producers
// with the specified configuration. This factory pattern allows for flexible producer creation with
// configuration overrides at build time.
//
// Parameters:
//   - config: A pointer to ProducerConfig containing the base configuration for producers created by this provider.
//     This includes settings like bootstrap servers, transactional parameters, and librdkafka-specific options.
//
// Returns:
//   - kafka.ProducerProvider: An implementation of the ProducerProvider interface that encapsulates the producer
//     creation logic using the librdkafka adapter.
//
// Example:
//
//	config := librd.NewProducerConfig()
//	config.BootstrapServers = []string{"localhost:9092"}
//	provider := librd.NewProducerProvider(config)
//	builder := provider.NewBuilder(&kafka.ProducerConfig{})
//	producer, err := builder(func(c *kafka.ProducerConfig) {
//	    // Apply custom configuration overrides here
//	})
func NewProducerProvider(config *ProducerConfig) kafka.ProducerProvider {
	return &producerProvider{config: config}
}

// NewBuilder creates a new ProducerBuilder configured with the provided kafka.ProducerConfig.
//
// This method initializes the librdkafka-specific channel sizes for event and produce operations,
// and returns a builder function that can be used to create producers with optional configuration overrides.
//
// Parameters:
//   - conf: A pointer to kafka.ProducerConfig containing the base producer configuration.
//     This configuration will be associated with the provider's internal config.
//
// Returns:
//   - kafka.ProducerBuilder: A builder function that accepts a configuration function and returns
//     a configured Producer instance or an error. The builder allows for
//     additional configuration customization at producer creation time.
//
// Example:
//
//	provider := librd.NewProducerProvider(baseConfig)
//	builder := provider.NewBuilder(&kafka.ProducerConfig{})
//	producer, err := builder(func(c *kafka.ProducerConfig) {
//	    // Apply custom overrides here
//	    c.Id = "my-custom-producer"
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
func (c *producerProvider) NewBuilder(conf *kafka.ProducerConfig) kafka.ProducerBuilder {
	c.config.ProducerConfig = conf

	if err := c.config.Librd.SetKey(`go.events.channel.size`, 1000); err != nil {
		panic(err)
	}

	if err := c.config.Librd.SetKey(`go.produce.channel.size`, 1000); err != nil {
		panic(err)
	}

	return func(configure func(*kafka.ProducerConfig)) (kafka.Producer, error) {
		defaultConfCopy := c.config.copy()
		configure(defaultConfCopy.ProducerConfig)

		return NewProducer(defaultConfCopy)
	}
}

func NewProducer(configs *ProducerConfig) (kafka.Producer, error) {
	if err := configs.setUp(); err != nil {
		return nil, errors.Wrap(err, `producer configs setup failed`)
	}

	if err := configs.validate(); err != nil {
		return nil, errors.Wrap(err, `invalid producer configs`)
	}

	loggerPrefix := `Producer`
	if configs.Transactional.Enabled {
		loggerPrefix = `TransactionalProducer`
	}

	configs.Logger = configs.Logger.NewLog(log.Prefixed(fmt.Sprintf(`%s(librdkafka)`, loggerPrefix)))

	configs.Logger.Info(`Producer creating...`)
	producer, err := librdKafka.NewProducer(configs.Librd)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`Producer(%s) init failed`, configs.Id))
	}

	defer configs.Logger.Info(`Producer created`)

	p := &Producer{
		config:          configs,
		baseProducer:    producer,
		partitionCounts: new(sync.Map),
	}

	go p.handleEvents()
	go p.printLogs()

	producerType := map[bool]string{true: `Y`, false: `N`}
	constLabels := map[string]string{`transactional`: producerType[p.config.Transactional.Enabled]}

	p.metrics.produceLatency = configs.MetricsReporter.Observer(metrics.MetricConf{
		Path:        `producer_produced_latency_microseconds`,
		Labels:      []string{`topic`},
		ConstLabels: constLabels,
	})

	p.metrics.transactions.commitLatency = configs.MetricsReporter.Observer(metrics.MetricConf{
		Path:        `producer_transaction_commit_latency_microseconds`,
		ConstLabels: constLabels,
	})

	p.metrics.transactions.initLatency = configs.MetricsReporter.Observer(metrics.MetricConf{
		Path:        `producer_transaction_init_latency_microseconds`,
		ConstLabels: constLabels,
	})

	p.metrics.transactions.abortLatency = configs.MetricsReporter.Observer(metrics.MetricConf{
		Path:        `producer_transaction_abort_latency_microseconds`,
		ConstLabels: constLabels,
	})

	p.metrics.produceErrors = configs.MetricsReporter.Counter(metrics.MetricConf{
		Path:        `producer_error_count`,
		Labels:      []string{`error`},
		ConstLabels: constLabels,
	})

	if p.config.Transactional.Enabled {
		return &TransactionalProducer{
			Producer: p,
			txBegin:  false,
		}, nil
	}

	return p, nil
}

func (p *Producer) NewRecord(
	ctx context.Context,
	key []byte,
	value []byte,
	topic string,
	partition int32,
	timestamp time.Time,
	headers kafka.RecordHeaders,
	meta string) kafka.Record {

	librdHeaders := make([]librdKafka.Header, len(headers))
	for i := range headers {
		librdHeaders[i] = librdKafka.Header{
			Key:   string(headers[i].Key),
			Value: headers[i].Value,
		}
	}

	return &Record{
		librd: &librdKafka.Message{
			TopicPartition: librdKafka.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Metadata:  &meta,
			},
			Value:     value,
			Key:       key,
			Timestamp: timestamp,
			Headers:   librdHeaders,
		},
		ctx: ctx,
	}
}

func (p *Producer) ProduceSync(ctx context.Context, message kafka.Record) (partition int32, offset int64, err error) {
	dChan := make(chan librdKafka.Event)
	kMessage, err := p.prepareMessage(message)
	if err != nil {
		return 0, 0, errors.Wrapf(err, `message[%s] prepare error`, message)
	}

	err = p.baseProducer.Produce(kMessage, dChan)
	if err != nil {
		return 0, 0, errors.Wrap(err, `cannot send message`)
	}

	dRpt := <-dChan
	dmSg := dRpt.(*librdKafka.Message)

	if dmSg.TopicPartition.Error != nil {
		return 0, 0, errors.Wrapf(dmSg.TopicPartition.Error, `message %s delivery failed`, message)
	}

	p.metrics.produceLatency.Observe(float64(time.Since(kMessage.Timestamp).Nanoseconds()/1e3), map[string]string{
		`topic`: *dmSg.TopicPartition.Topic,
	})

	p.config.Logger.DebugContext(ctx, fmt.Sprintf("Delivered message to topic %s[%d]@%d",
		message.Topic(), dmSg.TopicPartition.Partition, dmSg.TopicPartition.Offset))

	return dmSg.TopicPartition.Partition, int64(dmSg.TopicPartition.Offset), nil
}

func (p *Producer) Flush() {
	before := p.baseProducer.Len()
	remaining := p.baseProducer.Flush(10000)
	println(`before `, before, `after `, p.baseProducer.Len(), `remaining `, remaining)
	if err := p.baseProducer.Purge(
		librdKafka.PurgeInFlight |
			librdKafka.PurgeNonBlocking | librdKafka.PurgeQueue); err != nil {
		p.config.Logger.Error(err)
	}
}

func (p *Producer) Close() error {
	return p.close(false)
}

func (p *Producer) Restart() error {
	p.config.Logger.Info(`Producer restarting...`)
	defer p.config.Logger.Info(`Producer restarted`)

	if err := p.close(true); err != nil {
		p.config.Logger.Warn(err)
	}

	prd, err := librdKafka.NewProducer(p.config.Librd)
	if err != nil {
		p.config.Logger.Fatal(err)
	}

	p.baseProducer = prd

	p.handleEvents()

	p.printLogs()

	return nil
}

func (p *Producer) printLogs() {
	p.logsChannelClosed = make(chan struct{})

	go func() {
		defer p.config.Logger.Info(`Logs goroutine closed`)
		logger := p.config.Logger.NewLog(log.Prefixed(`LibrdLogs`))
		for lg := range p.baseProducer.Logs() {
			switch lg.Level {
			case 0, 1, 2:
				logger.Error(lg.String(), `level`, lg.Level)
			case 3, 4, 5:
				logger.Warn(lg.String(), `level`, lg.Level)
			case 6:
				logger.Info(lg.String(), `level`, lg.Level)
			case 7:
				logger.Debug(lg.String(), `level`, lg.Level)
			}
		}

		close(p.logsChannelClosed)
	}()
}

func (p *Producer) handleEvents() {
	// Capture message delivery reports, errors and metrics
	go func() {
		p.eventsChanClosed = make(chan struct{})
		defer p.config.Logger.Info(`Events goroutine closed`)

		for v := range p.baseProducer.Events() {
			switch event := v.(type) {
			case librdKafka.Error:
				p.config.Logger.Error(fmt.Sprintf(`Event [%s]%s`, event.Code(), event))
			case *librdKafka.Message:
				p.config.OnMessageDelivery(DeliveryReport{event})

			case *librdKafka.Stats:
				p.config.Logger.Error(fmt.Sprintf(`Event %s`, event))
			}
		}

		close(p.eventsChanClosed)
	}()
}

func (p *Producer) close(forced bool) error {
	if forced {
		p.config.Logger.Warn(`Producer closing forcefully...`)
	} else {
		p.config.Logger.Info(`Producer closing...`)
	}

	defer p.config.Logger.Info(`Producer closed`)

	err := p.baseProducer.AbortTransaction(nil)
	if err != nil {
		if err.(librdKafka.Error).Code() == librdKafka.ErrState {
			// No transaction in progress, ignore the error.
			err = nil
		} else {
			p.config.Logger.Warn(fmt.Sprintf(`Transaction abort error due to %s`, err.Error()))
		}
	}

	if forced {
		p.config.Logger.Info(`Purging producer queues kafka.PurgeInFlight|kafka.PurgeQueue`)
		if err := p.baseProducer.Purge(
			librdKafka.PurgeInFlight | librdKafka.PurgeQueue); err != nil {
			p.config.Logger.Error(err)
		}
	}

	if !p.baseProducer.IsClosed() {
		p.baseProducer.Close()
	}

	<-p.logsChannelClosed
	<-p.eventsChanClosed

	return nil
}

func (p *Producer) prepareMessage(message kafka.Record) (*librdKafka.Message, error) {
	t := time.Now()
	topic := message.Topic()
	m := &librdKafka.Message{
		TopicPartition: librdKafka.TopicPartition{
			Topic: &topic,
		},
		Key:           message.Key(),
		Value:         message.Value(),
		Timestamp:     t,
		TimestampType: librdKafka.TimestampCreateTime,
		Headers:       make([]librdKafka.Header, len(message.Headers())),
	}

	// Use the partitioner defined in librdkafka config
	if message.Partition() == kafka.PartitionAny {
		m.TopicPartition.Partition = librdKafka.PartitionAny
	}

	if message.Partition() > 0 {
		m.TopicPartition.Partition = message.Partition()
		goto Headers
	}

	if p.config.PartitionerFunc != nil {
		pCount, err := p.getPartitionCount(message.Topic())
		if err != nil {
			return nil, errors.Wrapf(err, `partition count failed for %d`, pCount)
		}

		m.TopicPartition.Partition, err = p.config.PartitionerFunc(message, pCount)
		if err != nil {
			return nil, errors.Wrapf(err, `partitioner error`)
		}
	}

Headers:
	for i, header := range message.Headers() {
		m.Headers[i] = librdKafka.Header{
			Key:   string(header.Key),
			Value: header.Value,
		}
	}

	if !message.Timestamp().IsZero() {
		m.Timestamp = message.Timestamp()
	}

	return m, nil
}

func (p *Producer) getPartitionCount(topic string) (int32, error) {
	//TODO refresh counts in a background thread
	v, ok := p.partitionCounts.Load(topic)
	if ok {
		return v.(int32), nil
	}

	meta, err := p.baseProducer.GetMetadata(&topic, false, 10000) //TODO make this configurable
	if err != nil {
		return 0, errors.Wrapf(err, `metadata fetch failed for %s`, topic)
	}

	if meta.Topics[topic].Error.Code() != librdKafka.ErrNoError {
		return 0, errors.Wrapf(meta.Topics[topic].Error, `metadata fetch failed for %s due to a topic error`, topic)
	}

	count := int32(len(meta.Topics[topic].Partitions))
	p.partitionCounts.Store(topic, count)

	return count, nil
}

func toLibrdLogLevel(level log.Level) int {
	switch level {
	case log.ERROR:
		return 2
	case log.WARN:
		return 5
	case log.INFO:
		return 6
	case log.DEBUG:
		return 7
	}

	return 0
}
