package kafka

import (
	"github.com/tryfix/log"
	"github.com/tryfix/metrics/v2"
)

type ProducerConfig struct {
	Id               string
	BootstrapServers []string
	PartitionerFunc  PartitionerFunc
	Acks             RequiredAcks
	Transactional    struct {
		Enabled bool
		Id      string
	}
	Idempotent        bool
	Logger            log.Logger
	MetricsReporter   metrics.Reporter
	MaxRetryCount     int
	OnMessageDelivery func(report DeliveryReport)
}

func (conf *ProducerConfig) Copy() *ProducerConfig {
	return &ProducerConfig{
		Id:                conf.Id,
		BootstrapServers:  conf.BootstrapServers,
		PartitionerFunc:   conf.PartitionerFunc,
		Acks:              conf.Acks,
		Transactional:     conf.Transactional,
		Idempotent:        conf.Idempotent,
		Logger:            conf.Logger,
		MetricsReporter:   conf.MetricsReporter,
		MaxRetryCount:     conf.MaxRetryCount,
		OnMessageDelivery: conf.OnMessageDelivery,
	}
}

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Acks:              WaitForAll,
		Logger:            log.NewNoopLogger(),
		MetricsReporter:   metrics.NoopReporter(),
		MaxRetryCount:     5,
		OnMessageDelivery: func(report DeliveryReport) {},
	}
}
