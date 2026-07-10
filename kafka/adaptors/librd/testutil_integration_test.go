/*//go:build integration
 */
package librd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	toxiproxyClient "github.com/Shopify/toxiproxy/v2/client"
	librdKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics/v2"
)

// =============================================================================
// Multi-broker Kafka Cluster Setup
// =============================================================================

// KafkaClusterBroker holds a single Kafka broker container
type KafkaClusterBroker struct {
	testcontainers.Container
	BootstrapServers string
}

// KafkaCluster holds multiple Kafka broker containers
type KafkaCluster struct {
	Brokers          []*KafkaClusterBroker
	BootstrapServers string
	Network          *testcontainers.DockerNetwork
}

// BrokerConfig holds configuration for individual broker
type BrokerConfig struct {
	NodeID     int
	Port       string
	CtrlPort   string
	ExternalIP string
}

// SetupMultiBrokerCluster creates a 3-broker KRaft Kafka cluster
// NOTE: All brokers must start in parallel for KRaft quorum to form
func SetupMultiBrokerCluster(ctx context.Context) (*KafkaCluster, error) {
	// Create a Docker network for the brokers
	net, err := network.New(ctx, network.WithCheckDuplicate())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker network: %w", err)
	}

	brokerConfigs := []BrokerConfig{
		{NodeID: 1, Port: "19092", CtrlPort: "29093", ExternalIP: "127.0.0.1"},
		{NodeID: 2, Port: "19093", CtrlPort: "29094", ExternalIP: "127.0.0.1"},
		{NodeID: 3, Port: "19094", CtrlPort: "29095", ExternalIP: "127.0.0.1"},
	}

	clusterId := "MkU3OEVBNTcwNTJENDM2Qk"
	quorumVoters := "1@kafka1:29093,2@kafka2:29094,3@kafka3:29095"

	brokers := make([]*KafkaClusterBroker, len(brokerConfigs))
	var wg sync.WaitGroup
	var mu sync.Mutex
	errChan := make(chan error, len(brokerConfigs))

	// Start all brokers in parallel - required for KRaft quorum to form
	for i, cfg := range brokerConfigs {
		wg.Add(1)
		go func(idx int, cfg BrokerConfig) {
			defer wg.Done()

			brokerName := fmt.Sprintf("kafka%d", cfg.NodeID)
			advertisedListeners := fmt.Sprintf("PLAINTEXT://%s:%s,BROKER://%s:9092", cfg.ExternalIP, cfg.Port, brokerName)

			req := testcontainers.ContainerRequest{
				Image:        "confluentinc/cp-kafka:7.5.0",
				ExposedPorts: []string{fmt.Sprintf("%s:9092/tcp", cfg.Port)},
				Networks:     []string{net.Name},
				NetworkAliases: map[string][]string{
					net.Name: {brokerName},
				},
				Name: brokerName,
				Env: map[string]string{
					"KAFKA_NODE_ID":                                  fmt.Sprintf("%d", cfg.NodeID),
					"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
					"KAFKA_ADVERTISED_LISTENERS":                     advertisedListeners,
					"KAFKA_PROCESS_ROLES":                            "broker,controller",
					"KAFKA_CONTROLLER_QUORUM_VOTERS":                 quorumVoters,
					"KAFKA_LISTENERS":                                fmt.Sprintf("PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:%s,BROKER://0.0.0.0:9093", cfg.CtrlPort),
					"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
					"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
					"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "3",
					"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "3",
					"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "2",
					"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
					"KAFKA_DEFAULT_REPLICATION_FACTOR":               "3",
					"KAFKA_MIN_INSYNC_REPLICAS":                      "2",
					"CLUSTER_ID":                                     clusterId,
				},
				WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(180 * time.Second),
			}

			container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: req,
				Started:          true,
			})
			if err != nil {
				errChan <- fmt.Errorf("failed to start broker %d: %w", cfg.NodeID, err)
				return
			}

			broker := &KafkaClusterBroker{
				Container:        container,
				BootstrapServers: fmt.Sprintf("%s:%s", cfg.ExternalIP, cfg.Port),
			}

			mu.Lock()
			brokers[idx] = broker
			mu.Unlock()
		}(i, cfg)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	var startupErrors []error
	for err := range errChan {
		startupErrors = append(startupErrors, err)
	}

	if len(startupErrors) > 0 {
		// Clean up any started containers
		for _, b := range brokers {
			if b != nil {
				b.Terminate(ctx)
			}
		}
		net.Remove(ctx)
		return nil, startupErrors[0]
	}

	// Build bootstrap servers string
	var bootstrapServers string
	for i, broker := range brokers {
		if i == 0 {
			bootstrapServers = broker.BootstrapServers
		} else {
			bootstrapServers += "," + broker.BootstrapServers
		}
	}

	return &KafkaCluster{
		Brokers:          brokers,
		BootstrapServers: bootstrapServers,
		Network:          net,
	}, nil
}

// Terminate stops all brokers and cleans up the network
func (c *KafkaCluster) Terminate(ctx context.Context) error {
	var errs []error
	for _, broker := range c.Brokers {
		if err := broker.Terminate(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if c.Network != nil {
		if err := c.Network.Remove(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors during cleanup: %v", errs)
	}
	return nil
}

// StopBroker stops a specific broker (0-indexed)
func (c *KafkaCluster) StopBroker(ctx context.Context, index int) error {
	if index < 0 || index >= len(c.Brokers) {
		return fmt.Errorf("invalid broker index: %d", index)
	}
	return c.Brokers[index].Stop(ctx, nil)
}

// StartBroker starts a previously stopped broker
func (c *KafkaCluster) StartBroker(ctx context.Context, index int) error {
	if index < 0 || index >= len(c.Brokers) {
		return fmt.Errorf("invalid broker index: %d", index)
	}
	return c.Brokers[index].Start(ctx)
}

// =============================================================================
// Toxiproxy Setup for Network Failure Simulation
// =============================================================================

// KafkaWithToxiproxy holds Kafka container with Toxiproxy for network simulation
type KafkaWithToxiproxy struct {
	Kafka      *KafkaClusterBroker
	ToxiproxyC *toxiproxy.Container
	Proxy      *toxiproxyClient.Proxy
	ProxyPort  string
	DirectPort string
	Network    *testcontainers.DockerNetwork
}

// setupKafkaWithToxiproxy creates a Kafka broker behind Toxiproxy for network failure simulation
func setupKafkaWithToxiproxy(ctx context.Context) (*KafkaWithToxiproxy, error) {
	// Create a Docker network
	net, err := network.New(ctx, network.WithCheckDuplicate())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker network: %w", err)
	}

	const proxyPort = "8666"

	// Start Toxiproxy container first - we need to know the mapped port before starting Kafka
	toxiproxyC, err := toxiproxy.Run(ctx, "ghcr.io/shopify/toxiproxy:2.9.0",
		network.WithNetwork([]string{"toxiproxy"}, net),
		testcontainers.WithExposedPorts(proxyPort+"/tcp"),
	)
	if err != nil {
		net.Remove(ctx)
		return nil, fmt.Errorf("failed to start toxiproxy: %w", err)
	}

	// Get the toxiproxy API URI and create a client
	toxiURI, err := toxiproxyC.URI(ctx)
	if err != nil {
		toxiproxyC.Terminate(ctx)
		net.Remove(ctx)
		return nil, fmt.Errorf("failed to get toxiproxy URI: %w", err)
	}
	toxiClient := toxiproxyClient.NewClient(toxiURI)

	// Create the proxy BEFORE Kafka starts so it's ready
	proxy, err := toxiClient.CreateProxy("kafka", "0.0.0.0:"+proxyPort, "kafka:9092")
	if err != nil {
		toxiproxyC.Terminate(ctx)
		net.Remove(ctx)
		return nil, fmt.Errorf("failed to create toxiproxy proxy: %w", err)
	}

	// Get the mapped port for the proxy on the host
	mappedPort, err := toxiproxyC.MappedPort(ctx, proxyPort+"/tcp")
	if err != nil {
		toxiproxyC.Terminate(ctx)
		net.Remove(ctx)
		return nil, fmt.Errorf("failed to get proxy port: %w", err)
	}

	toxiproxyHost, err := toxiproxyC.Host(ctx)
	if err != nil {
		toxiproxyC.Terminate(ctx)
		net.Remove(ctx)
		return nil, fmt.Errorf("failed to get toxiproxy host: %w", err)
	}

	// Now we know the external address - configure Kafka to advertise this
	// The client will connect to localhost:<mappedPort> which routes through Toxiproxy to kafka:9092
	advertisedListener := fmt.Sprintf("PLAINTEXT://%s:%s", toxiproxyHost, mappedPort.Port())

	// Start Kafka container (connected to the network, accessible via "kafka:9092" from toxiproxy)
	kafkaReq := testcontainers.ContainerRequest{
		Image:    "confluentinc/cp-kafka:7.5.0",
		Networks: []string{net.Name},
		NetworkAliases: map[string][]string{
			net.Name: {"kafka"},
		},
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_ADVERTISED_LISTENERS":                     advertisedListener, // Advertise the host-accessible address
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@kafka:29093",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"CLUSTER_ID":                                     "MkU3OEVBNTcwNTJENDM2Qk",
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(90 * time.Second),
	}

	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: kafkaReq,
		Started:          true,
	})
	if err != nil {
		toxiproxyC.Terminate(ctx)
		net.Remove(ctx)
		return nil, fmt.Errorf("failed to start kafka: %w", err)
	}

	bootstrapServers := fmt.Sprintf("%s:%s", toxiproxyHost, mappedPort.Port())

	return &KafkaWithToxiproxy{
		Kafka: &KafkaClusterBroker{
			Container:        kafkaContainer,
			BootstrapServers: bootstrapServers,
		},
		ToxiproxyC: toxiproxyC,
		Proxy:      proxy,
		ProxyPort:  mappedPort.Port(),
		DirectPort: "9092",
		Network:    net,
	}, nil
}

// DisableNetwork simulates a network partition by adding a timeout toxic
func (k *KafkaWithToxiproxy) DisableNetwork() error {
	if k.Proxy == nil {
		return fmt.Errorf("proxy not initialized")
	}
	// Add a timeout toxic that times out all connections (simulates network down)
	_, err := k.Proxy.AddToxic("cut_after_commit", "limit_data", "upstream", 1.0, toxiproxyClient.Attributes{
		"timeout": 1, // 1ms timeout effectively blocks all traffic
	})
	return err
}

// EnableNetwork removes the network partition
func (k *KafkaWithToxiproxy) EnableNetwork() error {
	if k.Proxy == nil {
		return fmt.Errorf("proxy not initialized")
	}
	return k.Proxy.RemoveToxic("network_down")
}

// AddLatency adds latency to network traffic
func (k *KafkaWithToxiproxy) AddLatency(latencyMs int) error {
	if k.Proxy == nil {
		return fmt.Errorf("proxy not initialized")
	}
	_, err := k.Proxy.AddToxic("latency", "latency", "downstream", 1.0, toxiproxyClient.Attributes{
		"latency": latencyMs,
	})
	return err
}

// RemoveLatency removes the latency toxic
func (k *KafkaWithToxiproxy) RemoveLatency() error {
	if k.Proxy == nil {
		return fmt.Errorf("proxy not initialized")
	}
	return k.Proxy.RemoveToxic("latency")
}

// BlockDownstream blocks all downstream traffic (responses from broker to client)
// while allowing upstream traffic (requests from client to broker) to flow freely.
// This simulates the scenario where a request reaches the broker and is processed,
// but the response never makes it back to the client (in-doubt transaction).
func (k *KafkaWithToxiproxy) BlockDownstream() error {
	if k.Proxy == nil {
		return fmt.Errorf("proxy not initialized")
	}
	// limit_data with bytes=0 on downstream blocks ALL responses immediately
	// Upstream (requests) are unaffected and flow freely to the broker
	_, err := k.Proxy.AddToxic("block_downstream", "limit_data", "downstream", 1.0, toxiproxyClient.Attributes{
		"bytes": 0, // Block all downstream traffic immediately
	})
	return err
}

// UnblockDownstream removes the downstream block
func (k *KafkaWithToxiproxy) UnblockDownstream() error {
	if k.Proxy == nil {
		return fmt.Errorf("proxy not initialized")
	}
	return k.Proxy.RemoveToxic("block_downstream")
}

// AddDownstreamLatency adds high latency to downstream traffic (responses only).
// This allows requests to flow normally and be processed by the broker,
// but delays the response so the client times out before receiving it.
// This is the most reliable way to simulate "commit succeeds but ACK fails".
func (k *KafkaWithToxiproxy) AddDownstreamLatency(latencyMs int) error {
	if k.Proxy == nil {
		return fmt.Errorf("proxy not initialized")
	}
	_, err := k.Proxy.AddToxic("downstream_latency", "latency", "downstream", 1.0, toxiproxyClient.Attributes{
		"latency": latencyMs,
	})
	return err
}

// RemoveDownstreamLatency removes the downstream latency toxic
func (k *KafkaWithToxiproxy) RemoveDownstreamLatency() error {
	if k.Proxy == nil {
		return fmt.Errorf("proxy not initialized")
	}
	return k.Proxy.RemoveToxic("downstream_latency")
}

// AddBandwidthLimit limits bandwidth to simulate slow network
func (k *KafkaWithToxiproxy) AddBandwidthLimit(bytesPerSecond int) error {
	if k.Proxy == nil {
		return fmt.Errorf("proxy not initialized")
	}
	_, err := k.Proxy.AddToxic("bandwidth", "bandwidth", "downstream", 1.0, toxiproxyClient.Attributes{
		"rate": bytesPerSecond,
	})
	return err
}

// RemoveBandwidthLimit removes the bandwidth toxic
func (k *KafkaWithToxiproxy) RemoveBandwidthLimit() error {
	if k.Proxy == nil {
		return fmt.Errorf("proxy not initialized")
	}
	return k.Proxy.RemoveToxic("bandwidth")
}

// Terminate cleans up all containers and network
func (k *KafkaWithToxiproxy) Terminate(ctx context.Context) error {
	var errs []error
	if k.Kafka != nil {
		if err := k.Kafka.Terminate(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if k.ToxiproxyC != nil {
		if err := k.ToxiproxyC.Terminate(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if k.Network != nil {
		if err := k.Network.Remove(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors during cleanup: %v", errs)
	}
	return nil
}

// =============================================================================
// Error Classification Helpers
// =============================================================================

// assertErrorClassification verifies the error has expected classification
func assertErrorClassification(t *testing.T, err error, expectedShutdown, expectedRestart, expectedAbort bool) {
	t.Helper()

	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	producerErr, ok := err.(Err)
	if !ok {
		t.Fatalf("Expected Err type, got %T: %v", err, err)
	}

	t.Logf("Error: %v", err)
	t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
		producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

	if producerErr.ShouldShutdown() != expectedShutdown {
		t.Errorf("ShouldShutdown() = %v, want %v", producerErr.ShouldShutdown(), expectedShutdown)
	}
	if producerErr.RequiresRestart() != expectedRestart {
		t.Errorf("RequiresRestart() = %v, want %v", producerErr.RequiresRestart(), expectedRestart)
	}
	if producerErr.TxnRequiresAbort() != expectedAbort {
		t.Errorf("TxnRequiresAbort() = %v, want %v", producerErr.TxnRequiresAbort(), expectedAbort)
	}
}

// assertAnyError verifies that an error occurred (any classification)
func assertAnyError(t *testing.T, err error) Err {
	t.Helper()

	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	producerErr, ok := err.(Err)
	if !ok {
		t.Fatalf("Expected Err type, got %T: %v", err, err)
	}

	t.Logf("Error: %v", err)
	t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
		producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

	return producerErr
}

// =============================================================================
// Broker Lifecycle Helpers
// =============================================================================

// waitForBrokerDown waits until the broker becomes unavailable
func waitForBrokerDown(ctx context.Context, bootstrapServers string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		producer, err := librdKafka.NewProducer(&librdKafka.ConfigMap{
			"bootstrap.servers":  bootstrapServers,
			"socket.timeout.ms":  1000,
			"message.timeout.ms": 1000,
		})
		if err != nil {
			return nil // Broker is down
		}

		// Try to get metadata
		_, err = producer.GetMetadata(nil, true, 1000)
		producer.Close()
		if err != nil {
			return nil // Broker is down
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("broker still up after %v", timeout)
}

// waitForBrokerUp waits until the broker becomes available
func waitForBrokerUp(ctx context.Context, bootstrapServers string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		producer, err := librdKafka.NewProducer(&librdKafka.ConfigMap{
			"bootstrap.servers":  bootstrapServers,
			"socket.timeout.ms":  2000,
			"message.timeout.ms": 2000,
		})
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Try to get metadata
		_, err = producer.GetMetadata(nil, true, 2000)
		producer.Close()
		if err == nil {
			return nil // Broker is up
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("broker still down after %v", timeout)
}

// createTxProducerWithConfig creates a transactional producer with custom librdkafka config
func createTxProducerWithConfig(t *testing.T, bootstrapServers, txId string, extraConfig map[string]interface{}) (*TransactionalProducer, func()) {
	t.Helper()

	config := NewProducerConfig()
	config.Id = "integration-test-tx-producer"
	config.BootstrapServers = []string{bootstrapServers}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = txId

	// Apply extra configuration
	for key, value := range extraConfig {
		config.Librd.SetKey(key, value)
	}

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	txProducer := producer.(*TransactionalProducer)
	cleanup := func() {
		producer.Close()
	}

	return txProducer, cleanup
}

// =============================================================================
// Consumer Group Helpers
// =============================================================================

// createConsumerAndJoinGroup creates a consumer and joins a group
func createConsumerAndJoinGroup(bootstrapServers, groupId, topic string) (*librdKafka.Consumer, error) {
	consumer, err := librdKafka.NewConsumer(&librdKafka.ConfigMap{
		"bootstrap.servers":     bootstrapServers,
		"group.id":              groupId,
		"auto.offset.reset":     "earliest",
		"enable.auto.commit":    false,
		"session.timeout.ms":    10000,
		"heartbeat.interval.ms": 3000,
		"max.poll.interval.ms":  30000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	// Poll to trigger group join
	for i := 0; i < 10; i++ {
		consumer.Poll(1000)
	}

	return consumer, nil
}

func assertError(t *testing.T, err kafka.ProducerErr, shouldAbort, shouldRestart, shouldShutdown bool) {
	t.Helper()

	var failed bool

	if shouldAbort != err.TxnRequiresAbort() {
		failed = true
	}

	if shouldRestart != err.RequiresRestart() {
		failed = true
	}

	if shouldShutdown != err.ShouldShutdown() {
		failed = true
	}

	if failed {
		t.Errorf(`Error assertion failed. 
							ShouldShutdown(Expected:%t Have:%t), 
							ShouldRestart(Expected:%t Have:%t), 
							TxnRequiresAbort(Expected:%t Have:%t)`,
			shouldShutdown, err.ShouldShutdown(),
			shouldRestart, err.RequiresRestart(),
			shouldAbort, err.TxnRequiresAbort())
	}
}

// =============================================================================
// Topic Management Helpers
// =============================================================================

// createTopic creates a topic with the specified configuration
func createTopic(ctx context.Context, bootstrapServers, topicName string, partitions, replicationFactor int) error {
	return createTopicWithConfig(ctx, bootstrapServers, topicName, partitions, replicationFactor, nil)
}

// createTopicWithConfig creates a topic with custom configuration
func createTopicWithConfig(ctx context.Context, bootstrapServers, topicName string, partitions, replicationFactor int, config map[string]string) error {
	adminClient, err := librdKafka.NewAdminClient(&librdKafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer adminClient.Close()

	topicSpec := librdKafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		Config:            config,
	}

	results, err := adminClient.CreateTopics(ctx, []librdKafka.TopicSpecification{topicSpec})
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	for _, result := range results {
		if result.Error.Code() != librdKafka.ErrNoError && result.Error.Code() != librdKafka.ErrTopicAlreadyExists {
			return fmt.Errorf("failed to create topic %s: %v", result.Topic, result.Error)
		}
	}

	return nil
}

// deleteTopic deletes a topic
func deleteTopic(ctx context.Context, bootstrapServers, topicName string) error {
	adminClient, err := librdKafka.NewAdminClient(&librdKafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer adminClient.Close()

	results, err := adminClient.DeleteTopics(ctx, []string{topicName})
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	for _, result := range results {
		if result.Error.Code() != librdKafka.ErrNoError && result.Error.Code() != librdKafka.ErrUnknownTopicOrPart {
			return fmt.Errorf("failed to delete topic %s: %v", result.Topic, result.Error)
		}
	}

	return nil
}

// =============================================================================
// Custom Kafka Protocol proxy Setup
// =============================================================================
