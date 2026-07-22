package mocks

import (
	"context"
	"fmt"
	"testing"
	"time"

	librdKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka/mocks/proxy"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/tryfix/log"
)

// KafkaCluster holds the Kafka cluster setup
type KafkaCluster struct {
	Containers      []testcontainers.Container
	proxy           *proxy.KafkaProtocolProxy
	BootstrapAddr   string // Address clients should connect to (proxy if enabled, otherwise direct)
	DirectKafkaAddr string // Direct Kafka address (bypassing proxy)
	config          *clusterConfig
}

// clusterConfig holds all configuration options
type clusterConfig struct {
	// Kafka settings
	kafkaVersion string
	nodeCount    int

	// proxy settings
	proxyEnabled bool
	proxyAddr    string
	directAddr   string

	// Replication settings
	replicationFactor int
	minISR            int

	// Timeouts
	startupTimeout time.Duration

	// Logging
	logger log.Logger
}

// Option is a functional option for configuring KafkaCluster
type Option func(*clusterConfig)

// WithVersion sets the Kafka version (Docker image tag)
// Example: WithVersion("7.5.0"), WithVersion("7.4.0"), WithVersion("6.2.0")
func WithVersion(version string) Option {
	return func(c *clusterConfig) {
		c.kafkaVersion = version
	}
}

// WithProxy enables the protocol-aware proxy between clients and Kafka.
// proxyAddr: address the proxy listens on (e.g., "127.0.0.1:19093")
// directAddr: address Kafka listens on for direct access (e.g., "127.0.0.1:19094")
//
// The proxy allows testing failure scenarios like:
//   - Dropping EndTxn responses (in-doubt transactions)
//   - Injecting error codes
//   - Delaying responses
func WithProxy(proxyAddr, directAddr string) Option {
	return func(c *clusterConfig) {
		c.proxyEnabled = true
		c.proxyAddr = proxyAddr
		c.directAddr = directAddr
	}
}

// WithNodes sets the number of Kafka broker nodes.
// For multi-node clusters, replication settings are automatically adjusted.
func WithNodes(count int) Option {
	return func(c *clusterConfig) {
		c.nodeCount = count
		// Adjust replication settings for multi-node
		if count > 1 {
			c.replicationFactor = minInt(count, 3)
			c.minISR = minInt(count-1, 2)
		}
	}
}

// WithReplication sets custom replication factor and min ISR.
// This overrides the automatic settings from WithNodes.
func WithReplication(replicationFactor, minISR int) Option {
	return func(c *clusterConfig) {
		c.replicationFactor = replicationFactor
		c.minISR = minISR
	}
}

// WithStartupTimeout sets the maximum time to wait for Kafka to start.
func WithStartupTimeout(timeout time.Duration) Option {
	return func(c *clusterConfig) {
		c.startupTimeout = timeout
	}
}

// WithLogger sets the logger for the cluster and proxy.
// Use NewTestLogger(t) to route all logs through testing.T.Log().
func WithLogger(logger log.Logger) Option {
	return func(c *clusterConfig) {
		c.logger = logger
	}
}

// defaultConfig returns the default configuration
func defaultConfig() *clusterConfig {
	return &clusterConfig{
		//kafkaVersion:      "7.5.0",
		kafkaVersion:      "8.2.2",
		nodeCount:         1,
		proxyEnabled:      false,
		proxyAddr:         "127.0.0.1:19093",
		directAddr:        "127.0.0.1:19094",
		replicationFactor: 1,
		minISR:            1,
		startupTimeout:    90 * time.Second,
	}
}

// SetupKafkaCluster creates a Kafka cluster with the given options.
//
// Examples:
//
//	// Simple single-node cluster (no proxy)
//	cluster, err := mocks.SetupKafkaCluster(t)
//
//	// Single-node with proxy for testing failures
//	cluster, err := mocks.SetupKafkaCluster(t,
//	    mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
//	)
//
//	// Specific Kafka version
//	cluster, err := mocks.SetupKafkaCluster(t,
//	    mocks.WithVersion("7.4.0"),
//	)
//
//	// Multi-node cluster for replication testing
//	cluster, err := mocks.SetupKafkaCluster(t,
//	    mocks.WithNodes(3),
//	)
func SetupKafkaCluster(t *testing.T, opts ...Option) (*KafkaCluster, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.nodeCount > 1 {
		return setupMultiNodeCluster(t, cfg)
	}
	return setupSingleNodeCluster(t, cfg)
}

func setupSingleNodeCluster(t *testing.T, cfg *clusterConfig) (*KafkaCluster, error) {
	var proxyInstance *proxy.KafkaProtocolProxy
	var bootstrapAddr, directKafkaAddr string

	if cfg.proxyEnabled {
		// Start proxy first
		proxyInstance = proxy.NewKafkaProtocolProxy(cfg.proxyAddr, cfg.directAddr)
		if cfg.logger != nil {
			proxyInstance.SetLogger(cfg.logger)
		}
		if err := proxyInstance.Start(); err != nil {
			return nil, fmt.Errorf("failed to start proxy: %w", err)
		}
		bootstrapAddr = cfg.proxyAddr
		directKafkaAddr = cfg.directAddr
	} else {
		// No proxy - clients connect directly
		bootstrapAddr = cfg.directAddr
		directKafkaAddr = cfg.directAddr
	}

	// Determine the port Kafka should expose
	kafkaPort := extractPort(cfg.directAddr)

	// Build Kafka container configuration
	env := buildSingleNodeEnv(cfg, bootstrapAddr)

	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("confluentinc/cp-kafka:%s", cfg.kafkaVersion),
		ExposedPorts: []string{fmt.Sprintf("%s:9092/tcp", kafkaPort)},
		Env:          env,
		WaitingFor:   wait.ForLog("Kafka Server started").WithStartupTimeout(cfg.startupTimeout),
	}

	t.Logf("Setting up Kafka %s (single node)...", cfg.kafkaVersion)
	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		if proxyInstance != nil {
			proxyInstance.Stop()
		}
		return nil, fmt.Errorf("failed to start Kafka container: %w", err)
	}

	t.Logf("Kafka bootstrap address: %s", bootstrapAddr)
	if cfg.proxyEnabled {
		t.Logf("Kafka direct address (bypassing proxy): %s", directKafkaAddr)
	}

	return &KafkaCluster{
		Containers:      []testcontainers.Container{container},
		proxy:           proxyInstance,
		BootstrapAddr:   bootstrapAddr,
		DirectKafkaAddr: directKafkaAddr,
		config:          cfg,
	}, nil
}

func setupMultiNodeCluster(t *testing.T, cfg *clusterConfig) (*KafkaCluster, error) {
	// TODO: Implement multi-node cluster setup
	// This would involve:
	// 1. Creating a Docker network
	// 2. Starting multiple Kafka containers with KRaft
	// 3. Configuring controller quorum voters
	// 4. Optionally putting proxy in front of all nodes
	return nil, fmt.Errorf("multi-node cluster not yet implemented (requested %d nodes)", cfg.nodeCount)
}

func buildSingleNodeEnv(cfg *clusterConfig, advertisedAddr string) map[string]string {
	env := map[string]string{
		"KAFKA_NODE_ID":                  "1",
		"KAFKA_PROCESS_ROLES":            "broker,controller",
		"KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:29093",
		"CLUSTER_ID":                     "MkU3OEVBNTcwNTJENDM2Qk",

		// Replication settings
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         fmt.Sprintf("%d", cfg.replicationFactor),
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": fmt.Sprintf("%d", cfg.replicationFactor),
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            fmt.Sprintf("%d", cfg.minISR),
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
	}

	if cfg.proxyEnabled {
		// With proxy: need INTERNAL listener for inter-broker + EXTERNAL through proxy
		// IMPORTANT: The INTERNAL listener is required for TransactionCoordinator
		// to communicate with itself inside the container.
		env["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
		env["KAFKA_LISTENERS"] = "INTERNAL://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093"
		env["KAFKA_ADVERTISED_LISTENERS"] = fmt.Sprintf("INTERNAL://localhost:9093,EXTERNAL://%s", advertisedAddr)
		env["KAFKA_INTER_BROKER_LISTENER_NAME"] = "INTERNAL"
		env["KAFKA_CONTROLLER_LISTENER_NAMES"] = "CONTROLLER"
	} else {
		// Without proxy: simpler listener configuration
		env["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
		env["KAFKA_LISTENERS"] = "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093"
		env["KAFKA_ADVERTISED_LISTENERS"] = fmt.Sprintf("PLAINTEXT://%s", advertisedAddr)
		env["KAFKA_INTER_BROKER_LISTENER_NAME"] = "PLAINTEXT"
		env["KAFKA_CONTROLLER_LISTENER_NAMES"] = "CONTROLLER"
	}

	return env
}

func extractPort(addr string) string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[i+1:]
		}
	}
	return "9092"
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Terminate cleans up all resources (proxy and containers)
func (k *KafkaCluster) Terminate(ctx context.Context) error {
	var errs []error

	if k.proxy != nil {
		if err := k.proxy.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("proxy stop: %w", err))
		}
	}

	for i, container := range k.Containers {
		if err := container.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("container %d terminate: %w", i, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

// BootstrapServers returns the bootstrap server addresses for clients to connect to
func (k *KafkaCluster) BootstrapServers() []string {
	return []string{k.BootstrapAddr}
}

// CreateTopic creates a topic with default replication settings
func (k *KafkaCluster) CreateTopic(t *testing.T, name string, partitions int) {
	k.CreateTopicWithConfig(t, name, partitions, k.config.replicationFactor, nil)
}

// CreateTopicWithConfig creates a topic with custom configuration
func (k *KafkaCluster) CreateTopicWithConfig(t *testing.T, name string, partitions, replicationFactor int, config map[string]string) {
	adminClient, err := librdKafka.NewAdminClient(&librdKafka.ConfigMap{
		"bootstrap.servers": k.DirectKafkaAddr,
	})
	if err != nil {
		t.Errorf("failed to create admin client: %s", err)
	}
	defer adminClient.Close()

	topicSpec := librdKafka.TopicSpecification{
		Topic:             name,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		Config:            config,
	}

	results, err := adminClient.CreateTopics(context.Background(), []librdKafka.TopicSpecification{topicSpec})
	if err != nil {
		t.Errorf("failed to create topic: %s", err)
	}

	for _, result := range results {
		if result.Error.Code() != librdKafka.ErrNoError && result.Error.Code() != librdKafka.ErrTopicAlreadyExists {
			t.Errorf("failed to create topic %s: %v", result.Topic, result.Error)
		}
	}
}

// DeleteTopic deletes a topic
func (k *KafkaCluster) DeleteTopic(ctx context.Context, name string) error {
	adminClient, err := librdKafka.NewAdminClient(&librdKafka.ConfigMap{
		"bootstrap.servers": k.DirectKafkaAddr,
	})
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer adminClient.Close()

	results, err := adminClient.DeleteTopics(ctx, []string{name})
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

// Proxy returns the proxy server
func (k *KafkaCluster) Proxy() *proxy.KafkaProtocolProxy {
	return k.proxy
}

// ProxyEnabled returns true if the proxy is enabled
func (k *KafkaCluster) ProxyEnabled() bool {
	return k.proxy != nil
}

// Version returns the Kafka version
func (k *KafkaCluster) Version() string {
	return k.config.kafkaVersion
}

// NodeCount returns the number of broker nodes
func (k *KafkaCluster) NodeCount() int {
	return k.config.nodeCount
}
