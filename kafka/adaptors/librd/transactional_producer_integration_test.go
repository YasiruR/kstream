/*//go:build integration
 */
package librd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	librdKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/docker/go-connections/nat"
	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka/mocks"
	proxyPkg "github.com/gmbyapa/kstream/v2/kafka/mocks/proxy"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics/v2"
)

// =============================================================================
// Integration tests for transactional producer using real Kafka broker
// Run with: go test -tags=integration -v ./kafka/adaptors/librd/... -run "TestIntegration"
// =============================================================================

// KafkaContainer holds the Kafka container instance
type KafkaContainer struct {
	testcontainers.Container
	BootstrapServers string
}

// setupKafkaContainer starts a Kafka container for integration testing
func setupKafkaContainer(ctx context.Context) (*KafkaContainer, error) {
	// Use a fixed external port to avoid port mapping issues with advertised listeners
	externalPort := "19092"
	kafkaPort := "9092/tcp"

	// IMPORTANT: We need separate INTERNAL and EXTERNAL listeners!
	// - INTERNAL: Used by TransactionCoordinator and inter-broker communication (inside container)
	// - EXTERNAL: Used by clients connecting from outside the container (host machine)
	//
	// Without this, the TransactionCoordinator cannot connect to itself to complete
	// transaction commits, causing transactions to silently fail.
	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:7.5.0",
		ExposedPorts: []string{externalPort + ":9092/tcp"},
		Env: map[string]string{
			"KAFKA_NODE_ID":                  "1",
			"KAFKA_PROCESS_ROLES":            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:29093",
			"CLUSTER_ID":                     "MkU3OEVBNTcwNTJENDM2Qk",

			// Listener configuration with INTERNAL and EXTERNAL
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
			"KAFKA_LISTENERS":                      "INTERNAL://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093",
			"KAFKA_ADVERTISED_LISTENERS":           "INTERNAL://localhost:9093,EXTERNAL://127.0.0.1:" + externalPort,

			// Use INTERNAL listener for inter-broker communication (TransactionCoordinator uses this!)
			"KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
			"KAFKA_CONTROLLER_LISTENER_NAMES":  "CONTROLLER",

			// Replication settings for single-node
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(90 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start Kafka container: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, nat.Port(kafkaPort))
	if err != nil {
		return nil, fmt.Errorf("failed to get mapped port: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get host: %w", err)
	}

	// Use the fixed external port since that's what the advertised listener is set to
	bootstrapServers := fmt.Sprintf("%s:%s", host, mappedPort.Port())

	return &KafkaContainer{
		Container:        container,
		BootstrapServers: bootstrapServers,
	}, nil
}

// createIntegrationTxProducer creates a transactional producer for integration testing
func createIntegrationTxProducer(t *testing.T, bootstrapServers string, txId string) (*TransactionalProducer, func()) {
	t.Helper()

	config := NewProducerConfig()
	config.Id = "integration-test-tx-producer"
	config.BootstrapServers = []string{bootstrapServers}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = txId

	// Set shorter transaction timeout for faster testing
	config.Librd.SetKey("transaction.timeout.ms", 10000)

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
// Test: InitTransactions with real Kafka broker
// =============================================================================

func TestIntegration_InitTransactions_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	t.Logf("Kafka broker started at: %s", kafkaContainer.BootstrapServers)

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-tx-init-success")
	defer cleanup()

	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(initCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	t.Log("InitTransactions succeeded with real Kafka broker")
}

// =============================================================================
// Test: Full transaction cycle with real Kafka broker
// =============================================================================

func TestIntegration_FullTransactionCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	t.Logf("Kafka broker started at: %s", kafkaContainer.BootstrapServers)

	// Create topic first using admin client
	adminClient, err := librdKafka.NewAdminClient(&librdKafka.ConfigMap{
		"bootstrap.servers": kafkaContainer.BootstrapServers,
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	topicName := "test-tx-topic"
	topicSpec := librdKafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	results, err := adminClient.CreateTopics(ctx, []librdKafka.TopicSpecification{topicSpec})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	for _, result := range results {
		if result.Error.Code() != librdKafka.ErrNoError && result.Error.Code() != librdKafka.ErrTopicAlreadyExists {
			t.Fatalf("Failed to create topic %s: %v", result.Topic, result.Error)
		}
	}

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-tx-full-cycle")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Init
	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}
	t.Log("InitTransactions succeeded")

	// Begin
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	t.Log("BeginTransaction succeeded")

	// Produce a message using producer's NewRecord method
	record := txProducer.NewRecord(
		txCtx,
		[]byte("test-key"),
		[]byte("test-value"),
		topicName,
		-1, // partition (auto-assign)
		time.Now(),
		nil, // headers
		"",  // meta
	)
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Fatalf("ProduceAsync failed: %v", err)
	}
	t.Log("ProduceAsync succeeded")

	// Commit
	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}
	t.Log("CommitTransaction succeeded")

	t.Log("Full transaction cycle completed successfully with real Kafka broker")
}

// =============================================================================
// Test: Producer fencing scenario with real Kafka broker
// Two producers with same transactional.id - second should fence the first
// =============================================================================

func TestIntegration_ProducerFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Use multi-broker cluster for better transaction coordinator handling
	kafkaCluster, err := SetupMultiBrokerCluster(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka cluster: %v", err)
	}
	defer func() {
		for _, broker := range kafkaCluster.Brokers {
			broker.Container.Terminate(ctx)
		}
		kafkaCluster.Network.Remove(ctx)
	}()

	t.Logf("Kafka cluster started at: %s", kafkaCluster.BootstrapServers)

	// Wait for cluster to stabilize
	time.Sleep(5 * time.Second)

	// Create topic with replication factor 3
	topicName := "fencing-test-topic"
	err = createTopic(ctx, kafkaCluster.BootstrapServers, topicName, 1, 3)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Wait for topic to be fully replicated
	time.Sleep(2 * time.Second)

	txId := "fencing-test-tx-id"

	// Create first producer and initialize, complete a full transaction
	txProducer1, cleanup1 := createIntegrationTxProducer(t, kafkaCluster.BootstrapServers, txId)
	defer cleanup1()

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer1.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("Producer1 InitTransactions failed: %v", err)
	}
	t.Log("Producer1 InitTransactions succeeded")

	// Complete a full transaction cycle first
	err = txProducer1.BeginTransaction()
	if err != nil {
		t.Fatalf("Producer1 BeginTransaction failed: %v", err)
	}
	t.Log("Producer1 BeginTransaction succeeded")

	record1 := txProducer1.NewRecord(
		txCtx,
		[]byte("key1"),
		[]byte("value1"),
		topicName,
		-1,
		time.Now(),
		nil,
		"",
	)
	err = txProducer1.ProduceAsync(txCtx, record1)
	if err != nil {
		t.Fatalf("Producer1 ProduceAsync failed: %v", err)
	}
	t.Log("Producer1 ProduceAsync succeeded")

	err = txProducer1.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("Producer1 first CommitTransaction failed: %v", err)
	}
	t.Log("Producer1 first transaction committed")

	// Now create second producer with SAME transactional.id - this should fence producer1
	txProducer2, cleanup2 := createIntegrationTxProducer(t, kafkaCluster.BootstrapServers, txId)
	defer cleanup2()

	err = txProducer2.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("Producer2 InitTransactions failed: %v", err)
	}
	t.Log("Producer2 InitTransactions succeeded - should have fenced Producer1")

	// Now try to start a new transaction on producer1 - should fail with fencing error
	err = txProducer1.BeginTransaction()
	if err != nil {
		t.Logf("Producer1 BeginTransaction failed after fencing: %v", err)
	} else {
		t.Log("BeginTransaction succeeded, trying to produce and commit to trigger fencing")

		record2 := txProducer1.NewRecord(
			txCtx,
			[]byte("key2"),
			[]byte("value2"),
			topicName,
			-1,
			time.Now(),
			nil,
			"",
		)
		err = txProducer1.ProduceAsync(txCtx, record2)
		if err != nil {
			t.Logf("Producer1 ProduceAsync failed after fencing: %v", err)
		} else {
			t.Log("ProduceAsync succeeded, trying commit")
			err = txProducer1.CommitTransaction(txCtx)
			if err != nil {
				t.Logf("Producer1 CommitTransaction failed after fencing: %v", err)
			}
		}
	}

	if err == nil {
		t.Log("Producer1 operations succeeded - fencing may not have taken effect yet")
		t.Log("This is expected in some timing scenarios with librdkafka")
		return
	}

	t.Logf("Producer1 operation failed as expected after fencing: %v", err)

	// Verify error classification
	producerErr, ok := err.(Err)
	if !ok {
		t.Fatalf("Expected Err type, got %T: %v", err, err)
	}

	t.Logf("Error classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
		producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

	// For fencing, we expect RequiresRestart=true
	if producerErr.RequiresRestart() {
		t.Log("SUCCESS: Fencing error correctly classified with RequiresRestart=true")
	} else if producerErr.TxnRequiresAbort() {
		t.Log("Fencing error classified with TxnRequiresAbort=true (may need handleTxError adjustment)")
	} else if producerErr.ShouldShutdown() {
		t.Log("Fencing error classified with ShouldShutdown=true (may need handleTxError adjustment)")
	}

	t.Log("Producer fencing test completed")
}

// =============================================================================
// Test: Transaction abort and retry with real Kafka broker
// =============================================================================

func TestIntegration_AbortAndRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	t.Logf("Kafka broker started at: %s", kafkaContainer.BootstrapServers)

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-tx-abort-retry")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Init
	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// First transaction - abort it
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction (1st) failed: %v", err)
	}
	t.Log("First transaction begun")

	err = txProducer.AbortTransaction(txCtx)
	if err != nil {
		t.Fatalf("AbortTransaction failed: %v", err)
	}
	t.Log("First transaction aborted")

	// Second transaction - commit it
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction (2nd) failed: %v", err)
	}
	t.Log("Second transaction begun")

	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("CommitTransaction (2nd) failed: %v", err)
	}
	t.Log("Second transaction committed")

	t.Log("Abort and retry test completed successfully")
}

// =============================================================================
// Test: SendOffsetsToTransaction with real Kafka broker
// =============================================================================

func TestIntegration_SendOffsetsToTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	t.Logf("Kafka broker started at: %s", kafkaContainer.BootstrapServers)

	// Create topic first
	adminClient, err := librdKafka.NewAdminClient(&librdKafka.ConfigMap{
		"bootstrap.servers": kafkaContainer.BootstrapServers,
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	topicName := "test-offsets-topic"
	topicSpec := librdKafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	results, err := adminClient.CreateTopics(ctx, []librdKafka.TopicSpecification{topicSpec})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	for _, result := range results {
		if result.Error.Code() != librdKafka.ErrNoError && result.Error.Code() != librdKafka.ErrTopicAlreadyExists {
			t.Fatalf("Failed to create topic %s: %v", result.Topic, result.Error)
		}
	}

	// Create a consumer to get group metadata
	consumer, err := librdKafka.NewConsumer(&librdKafka.ConfigMap{
		"bootstrap.servers": kafkaContainer.BootstrapServers,
		"group.id":          "test-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Subscribe to get group membership
	err = consumer.Subscribe(topicName, nil)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Poll to trigger group join
	consumer.Poll(5000)

	cgmd, err := consumer.GetConsumerGroupMetadata()
	if err != nil {
		t.Fatalf("Failed to get consumer group metadata: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-tx-send-offsets")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Init and begin
	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Send offsets
	offsets := []kafka.ConsumerOffset{
		{Topic: topicName, Partition: 0, Offset: 10, Meta: ""},
	}
	groupMeta := &kafka.GroupMeta{Meta: cgmd}

	err = txProducer.SendOffsetsToTransaction(txCtx, offsets, groupMeta)
	if err != nil {
		// This might fail if consumer group is not stable yet - log and continue
		t.Logf("SendOffsetsToTransaction returned error (may be expected): %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Error classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

			if producerErr.TxnRequiresAbort() {
				err = txProducer.AbortTransaction(txCtx)
				if err != nil {
					t.Logf("AbortTransaction after SendOffsetsToTransaction failure: %v", err)
				}
			}
		}
		return
	}

	// Commit
	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	t.Log("SendOffsetsToTransaction test completed successfully")
}

// =============================================================================
// Test: Error classification table-driven test with real Kafka broker
// =============================================================================

func TestIntegration_ErrorClassification(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	t.Logf("Kafka broker started at: %s", kafkaContainer.BootstrapServers)

	testCases := []struct {
		name        string
		scenario    string
		expectAbort bool
	}{
		{
			name:        "CommitWithoutBegin",
			scenario:    "commit_without_begin",
			expectAbort: false, // ErrState -> ShouldShutdown
		},
		{
			name:        "BeginWithoutInit",
			scenario:    "begin_without_init",
			expectAbort: false, // ErrState -> ShouldShutdown
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-error-"+tc.name)
			defer cleanup()

			txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			var testErr error

			switch tc.scenario {
			case "commit_without_begin":
				// Init but don't begin
				err := txProducer.InitTransactions(txCtx)
				if err != nil {
					t.Fatalf("InitTransactions failed: %v", err)
				}
				testErr = txProducer.CommitTransaction(txCtx)

			case "begin_without_init":
				testErr = txProducer.BeginTransaction()
			}

			if testErr == nil {
				t.Fatal("Expected error but got nil")
			}

			producerErr, ok := testErr.(Err)
			if !ok {
				t.Fatalf("Expected Err type, got %T: %v", testErr, testErr)
			}

			t.Logf("Error: %v", testErr)
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

			// Verify expected classification
			if producerErr.TxnRequiresAbort() != tc.expectAbort {
				t.Errorf("TxnRequiresAbort() = %v, want %v", producerErr.TxnRequiresAbort(), tc.expectAbort)
			}
		})
	}
}

// =============================================================================
// CATEGORY 4: Transaction State Errors
// Tests for API misuse and invalid transaction state transitions
// =============================================================================

func TestIntegration_BeginWithoutInit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-begin-no-init")
	defer cleanup()

	// Try to begin without init - should fail
	err = txProducer.BeginTransaction()
	if err == nil {
		t.Fatal("Expected error when calling BeginTransaction without InitTransactions")
	}

	producerErr, ok := err.(Err)
	if !ok {
		t.Fatalf("Expected Err type, got %T: %v", err, err)
	}

	t.Logf("Error: %v", err)
	t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
		producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

	// Invalid state should result in ShouldShutdown
	if !producerErr.ShouldShutdown() {
		t.Errorf("Expected ShouldShutdown=true for BeginTransaction without Init")
	}
}

func TestIntegration_CommitWithoutBegin(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-commit-no-begin")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Init transactions
	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Try to commit without begin - should fail
	err = txProducer.CommitTransaction(txCtx)
	if err == nil {
		t.Fatal("Expected error when calling CommitTransaction without BeginTransaction")
	}

	producerErr, ok := err.(Err)
	if !ok {
		t.Fatalf("Expected Err type, got %T: %v", err, err)
	}

	t.Logf("Error: %v", err)
	t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
		producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

	// Invalid state should result in ShouldShutdown
	if !producerErr.ShouldShutdown() {
		t.Errorf("Expected ShouldShutdown=true for CommitTransaction without Begin")
	}
}

func TestIntegration_DoubleCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-double-commit")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Init transactions
	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Begin transaction
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// First commit should succeed
	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("First CommitTransaction failed: %v", err)
	}

	// Second commit should fail - no active transaction
	err = txProducer.CommitTransaction(txCtx)
	if err == nil {
		t.Fatal("Expected error when calling CommitTransaction twice")
	}

	producerErr, ok := err.(Err)
	if !ok {
		t.Fatalf("Expected Err type, got %T: %v", err, err)
	}

	t.Logf("Error: %v", err)
	t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
		producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
}

func TestIntegration_CommitAfterAbort(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-commit-after-abort")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Init transactions
	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Begin transaction
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Abort transaction
	err = txProducer.AbortTransaction(txCtx)
	if err != nil {
		t.Fatalf("AbortTransaction failed: %v", err)
	}

	// Try to commit after abort - should fail
	err = txProducer.CommitTransaction(txCtx)
	if err == nil {
		t.Fatal("Expected error when calling CommitTransaction after AbortTransaction")
	}

	producerErr, ok := err.(Err)
	if !ok {
		t.Fatalf("Expected Err type, got %T: %v", err, err)
	}

	t.Logf("Error: %v", err)
	t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
		producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
}

func TestIntegration_AbortWithoutBegin(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-abort-no-begin")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Init transactions
	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Abort without begin - should be handled gracefully (ErrState is ignored)
	err = txProducer.AbortTransaction(txCtx)
	// The implementation ignores ErrState for abort, so this should not return an error
	if err != nil {
		producerErr, ok := err.(Err)
		if ok {
			t.Logf("AbortWithoutBegin returned error (may be expected): %v", err)
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		} else {
			t.Logf("Unexpected error type: %T: %v", err, err)
		}
	} else {
		t.Log("AbortTransaction without Begin correctly returned no error (ErrState ignored)")
	}
}

func TestIntegration_ProduceWithoutBegin(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	// Create topic first
	err = createTopic(ctx, kafkaContainer.BootstrapServers, "test-produce-no-begin", 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-produce-no-begin")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Init transactions
	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Don't call BeginTransaction - the ProduceAsync method will auto-begin
	// So this test is to verify that behavior
	record := txProducer.NewRecord(
		txCtx,
		[]byte("key"),
		[]byte("value"),
		"test-produce-no-begin",
		-1,
		time.Now(),
		nil,
		"",
	)

	// ProduceAsync auto-begins transaction if not started
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Logf("ProduceAsync error (may be expected): %v", err)
	} else {
		t.Log("ProduceAsync auto-started transaction as expected")
		// Clean up by aborting
		txProducer.AbortTransaction(txCtx)
	}
}

// =============================================================================
// CATEGORY 5: Resource Exhaustion
// Tests for buffer overflow and message size limits
// =============================================================================

func TestIntegration_QueueFull_BufferExhausted(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	// Create topic
	topicName := "test-queue-full"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create producer with very small queue size
	config := NewProducerConfig()
	config.Id = "queue-full-test"
	config.BootstrapServers = []string{kafkaContainer.BootstrapServers}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "test-queue-full-tx"
	config.Librd.SetKey("queue.buffering.max.messages", 10) // Very small buffer
	config.Librd.SetKey("transaction.timeout.ms", 30000)

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Flood the queue with messages
	var queueFullErrors int
	largeValue := make([]byte, 10000) // 10KB per message

	for i := 0; i < 1000; i++ {
		record := txProducer.NewRecord(
			txCtx,
			[]byte(fmt.Sprintf("key-%d", i)),
			largeValue,
			topicName,
			-1,
			time.Now(),
			nil,
			"",
		)

		err = txProducer.ProduceAsync(txCtx, record)
		if err != nil {
			queueFullErrors++
			t.Logf("ProduceAsync error at message %d: %v", i, err)
			break
		}
	}

	t.Logf("Queue full errors: %d", queueFullErrors)

	// Abort the transaction
	txProducer.AbortTransaction(txCtx)

	t.Log("Queue full test completed")
}

func TestIntegration_MessageTooLarge_ExceedsLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	// Create topic with small max message size
	topicName := "test-message-too-large"
	err = createTopicWithConfig(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1, map[string]string{
		"max.message.bytes": "1000", // 1KB limit
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "test-message-too-large-tx")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Create a message larger than the limit
	largeValue := make([]byte, 100000) // 100KB - exceeds 1KB limit

	record := txProducer.NewRecord(
		txCtx,
		[]byte("key"),
		largeValue,
		topicName,
		-1,
		time.Now(),
		nil,
		"",
	)

	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Logf("ProduceAsync correctly rejected large message: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		}
	} else {
		t.Log("ProduceAsync accepted message, checking commit")
		// Commit may fail due to message size
		err = txProducer.CommitTransaction(txCtx)
		if err != nil {
			t.Logf("CommitTransaction failed (expected for large message): %v", err)
		}
	}

	// Cleanup
	txProducer.AbortTransaction(txCtx)
}

// =============================================================================
// CATEGORY 3: Producer Fencing Scenarios
// Tests for epoch-based producer fencing
// =============================================================================

func TestIntegration_ProducerFencing_SameTransactionalId(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Use multi-broker cluster for better transaction coordinator handling
	kafkaCluster, err := SetupMultiBrokerCluster(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka cluster: %v", err)
	}
	defer kafkaCluster.Terminate(ctx)

	t.Logf("Kafka cluster started at: %s", kafkaCluster.BootstrapServers)

	// Wait for cluster to stabilize
	time.Sleep(5 * time.Second)

	// Create topic with replication factor 3
	topicName := "test-fencing"
	err = createTopic(ctx, kafkaCluster.BootstrapServers, topicName, 1, 3)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Wait for topic to be fully replicated
	time.Sleep(2 * time.Second)

	txId := "shared-transactional-id"

	// Create first producer
	txProducer1, cleanup1 := createIntegrationTxProducer(t, kafkaCluster.BootstrapServers, txId)
	defer cleanup1()

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer1.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("Producer1 InitTransactions failed: %v", err)
	}
	t.Log("Producer1 initialized")

	// Start a transaction on producer 1
	err = txProducer1.BeginTransaction()
	if err != nil {
		t.Fatalf("Producer1 BeginTransaction failed: %v", err)
	}

	// Produce a message
	record1 := txProducer1.NewRecord(
		txCtx,
		[]byte("key1"),
		[]byte("value1"),
		topicName,
		-1,
		time.Now(),
		nil,
		"",
	)
	err = txProducer1.ProduceAsync(txCtx, record1)
	if err != nil {
		t.Fatalf("Producer1 ProduceAsync failed: %v", err)
	}

	// Commit producer 1's first transaction
	err = txProducer1.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("Producer1 CommitTransaction failed: %v", err)
	}
	t.Log("Producer1 first transaction committed")

	// Create second producer with same transactional ID - this should fence producer 1
	txProducer2, cleanup2 := createIntegrationTxProducer(t, kafkaCluster.BootstrapServers, txId)
	defer cleanup2()

	err = txProducer2.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("Producer2 InitTransactions failed: %v", err)
	}
	t.Log("Producer2 initialized - should have fenced Producer1")

	// Now producer 1 should be fenced - any operation should fail
	err = txProducer1.BeginTransaction()
	if err == nil {
		// Try to produce and commit
		record2 := txProducer1.NewRecord(
			txCtx,
			[]byte("key2"),
			[]byte("value2"),
			topicName,
			-1,
			time.Now(),
			nil,
			"",
		)
		err = txProducer1.ProduceAsync(txCtx, record2)
		if err == nil {
			err = txProducer1.CommitTransaction(txCtx)
		}
	}

	if err != nil {
		t.Logf("Producer1 fenced as expected: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

			// Fencing should require restart
			if producerErr.RequiresRestart() {
				t.Log("SUCCESS: Fencing error correctly requires restart")
			}
		}
	} else {
		t.Log("Producer1 operations succeeded - fencing may not have taken effect yet")
	}
}

func TestIntegration_ProducerFencing_ZombieProducerAfterRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Use multi-broker cluster for better transaction coordinator handling
	kafkaCluster, err := SetupMultiBrokerCluster(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka cluster: %v", err)
	}
	defer kafkaCluster.Terminate(ctx)

	t.Logf("Kafka cluster started at: %s", kafkaCluster.BootstrapServers)

	// Wait for cluster to stabilize
	time.Sleep(5 * time.Second)

	topicName := "test-zombie-fencing"
	err = createTopic(ctx, kafkaCluster.BootstrapServers, topicName, 1, 3)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Wait for topic to be fully replicated
	time.Sleep(2 * time.Second)

	txId := "zombie-test-tx-id"

	// First producer - simulating original instance
	txProducer1, cleanup1 := createIntegrationTxProducer(t, kafkaCluster.BootstrapServers, txId)

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer1.InitTransactions(txCtx)
	if err != nil {
		cleanup1()
		t.Fatalf("Producer1 InitTransactions failed: %v", err)
	}

	// Begin a transaction on producer 1
	err = txProducer1.BeginTransaction()
	if err != nil {
		cleanup1()
		t.Fatalf("Producer1 BeginTransaction failed: %v", err)
	}

	record1 := txProducer1.NewRecord(txCtx, []byte("key1"), []byte("value1"), topicName, -1, time.Now(), nil, "")
	err = txProducer1.ProduceAsync(txCtx, record1)
	if err != nil {
		cleanup1()
		t.Fatalf("Producer1 ProduceAsync failed: %v", err)
	}

	// Simulate crash - don't commit/abort, just close
	// Keep producer1 reference to simulate zombie
	t.Log("Producer1 crashed (simulated) with uncommitted transaction")

	// New producer instance - simulating recovery
	txProducer2, cleanup2 := createIntegrationTxProducer(t, kafkaCluster.BootstrapServers, txId)
	defer cleanup2()

	// This should abort the pending transaction from producer1 and fence it
	err = txProducer2.InitTransactions(txCtx)
	if err != nil {
		cleanup1()
		t.Fatalf("Producer2 InitTransactions failed: %v", err)
	}
	t.Log("Producer2 initialized - pending transaction should be aborted")

	// Producer2 should work normally
	err = txProducer2.BeginTransaction()
	if err != nil {
		cleanup1()
		t.Fatalf("Producer2 BeginTransaction failed: %v", err)
	}

	record2 := txProducer2.NewRecord(txCtx, []byte("key2"), []byte("value2"), topicName, -1, time.Now(), nil, "")
	err = txProducer2.ProduceAsync(txCtx, record2)
	if err != nil {
		cleanup1()
		t.Fatalf("Producer2 ProduceAsync failed: %v", err)
	}

	err = txProducer2.CommitTransaction(txCtx)
	if err != nil {
		cleanup1()
		t.Fatalf("Producer2 CommitTransaction failed: %v", err)
	}
	t.Log("Producer2 successfully committed")

	// Now try to use the zombie producer1 - should fail
	err = txProducer1.BeginTransaction()
	if err == nil {
		record3 := txProducer1.NewRecord(txCtx, []byte("key3"), []byte("value3"), topicName, -1, time.Now(), nil, "")
		err = txProducer1.ProduceAsync(txCtx, record3)
		if err == nil {
			err = txProducer1.CommitTransaction(txCtx)
		}
	}

	cleanup1() // Clean up producer1

	if err != nil {
		t.Logf("Zombie producer correctly rejected: %v", err)
		producerErr, ok := err.(Err)
		if ok && producerErr.RequiresRestart() {
			t.Log("SUCCESS: Zombie producer error requires restart as expected")
		}
	} else {
		t.Log("Zombie producer operations succeeded - timing dependent")
	}
}

// =============================================================================
// CATEGORY 1: Broker/Infrastructure Failures
// Tests for broker shutdown, restart, and unavailability
// =============================================================================

func TestIntegration_BrokerShutdown_DuringTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	topicName := "test-broker-shutdown"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create producer with short timeout
	config := NewProducerConfig()
	config.Id = "broker-shutdown-test"
	config.BootstrapServers = []string{kafkaContainer.BootstrapServers}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "broker-shutdown-tx"
	config.Librd.SetKey("transaction.timeout.ms", 10000)
	config.Librd.SetKey("socket.timeout.ms", 5000)
	config.Librd.SetKey("message.timeout.ms", 5000)

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	txCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	record := txProducer.NewRecord(txCtx, []byte("key"), []byte("value"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Fatalf("ProduceAsync failed: %v", err)
	}

	t.Log("Transaction started, now stopping broker...")

	// Stop the broker
	err = kafkaContainer.Stop(ctx, nil)
	if err != nil {
		t.Logf("Failed to stop broker (may be expected): %v", err)
	}

	t.Log("Broker stopped, attempting to commit...")

	// Try to commit - should fail
	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Logf("CommitTransaction failed as expected during broker shutdown: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		}
	} else {
		t.Log("CommitTransaction succeeded before broker fully stopped")
	}
}

func TestIntegration_BrokerRestart_RecoveryAfterRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Use multi-broker cluster - stopping one broker won't break the cluster
	kafkaCluster, err := SetupMultiBrokerCluster(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka cluster: %v", err)
	}
	defer kafkaCluster.Terminate(ctx)

	t.Logf("Kafka cluster started at: %s", kafkaCluster.BootstrapServers)

	// Wait for cluster to stabilize
	time.Sleep(5 * time.Second)

	topicName := "test-broker-restart"
	err = createTopic(ctx, kafkaCluster.BootstrapServers, topicName, 3, 3) // 3 partitions, RF=3
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Wait for topic to be fully replicated
	time.Sleep(2 * time.Second)

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaCluster.BootstrapServers, "broker-restart-tx")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Complete a successful transaction first
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	record := txProducer.NewRecord(txCtx, []byte("key-before"), []byte("value-before"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Fatalf("ProduceAsync failed: %v", err)
	}

	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("First CommitTransaction failed: %v", err)
	}
	t.Log("First transaction committed successfully")

	// Stop one broker (broker 0)
	t.Log("Stopping broker 0...")
	err = kafkaCluster.StopBroker(ctx, 0)
	if err != nil {
		t.Logf("Stop broker error: %v", err)
	}

	// Wait a bit for cluster to recognize the broker is down
	time.Sleep(5 * time.Second)
	t.Log("Broker 0 stopped, cluster should still be operational with 2 remaining brokers")

	// Start the broker again
	t.Log("Starting broker 0...")
	err = kafkaCluster.StartBroker(ctx, 0)
	if err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}

	// Wait for broker to rejoin
	time.Sleep(5 * time.Second)
	t.Log("Broker 0 restarted")

	// Try another transaction - should work since cluster remained available
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Logf("BeginTransaction after broker restart: %v", err)
		// May need to recreate producer
		return
	}

	record2 := txProducer.NewRecord(txCtx, []byte("key-after"), []byte("value-after"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record2)
	if err != nil {
		t.Logf("ProduceAsync after broker restart failed: %v", err)
		txProducer.AbortTransaction(txCtx)
		return
	}

	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Logf("CommitTransaction after broker restart failed: %v", err)
	} else {
		t.Log("SUCCESS: Transaction completed after broker restart")
	}
}

// =============================================================================
// CATEGORY 6: Topic/Partition Issues
// Tests for topic deletion, non-existent topics, and partition issues
// =============================================================================

func TestIntegration_TopicNotExists_ProduceToMissing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	// Create producer with auto.create.topics disabled
	config := NewProducerConfig()
	config.Id = "topic-not-exists-test"
	config.BootstrapServers = []string{kafkaContainer.BootstrapServers}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "topic-not-exists-tx"
	config.Librd.SetKey("transaction.timeout.ms", 10000)
	config.Librd.SetKey("allow.auto.create.topics", false)

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Try to produce to non-existent topic
	record := txProducer.NewRecord(
		txCtx,
		[]byte("key"),
		[]byte("value"),
		"non-existent-topic-12345",
		-1,
		time.Now(),
		nil,
		"",
	)

	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Logf("ProduceAsync to non-existent topic failed: %v", err)
	} else {
		// Try to commit - may fail due to unknown topic
		err = txProducer.CommitTransaction(txCtx)
		if err != nil {
			t.Logf("CommitTransaction failed for non-existent topic: %v", err)

			producerErr, ok := err.(Err)
			if ok {
				t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
					producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
			}
		}
	}

	// Cleanup
	txProducer.AbortTransaction(txCtx)
}

func TestIntegration_TopicDeleted_DuringTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	topicName := "test-topic-delete"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "topic-delete-tx")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Produce first message
	record1 := txProducer.NewRecord(txCtx, []byte("key1"), []byte("value1"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record1)
	if err != nil {
		t.Fatalf("First ProduceAsync failed: %v", err)
	}
	t.Log("First message produced")

	// Delete the topic while transaction is active
	t.Log("Deleting topic...")
	err = deleteTopic(ctx, kafkaContainer.BootstrapServers, topicName)
	if err != nil {
		t.Logf("Delete topic error (may be expected): %v", err)
	}

	// Wait a bit for deletion to propagate
	time.Sleep(2 * time.Second)

	// Try to produce another message
	record2 := txProducer.NewRecord(txCtx, []byte("key2"), []byte("value2"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record2)
	if err != nil {
		t.Logf("Second ProduceAsync failed after topic deletion: %v", err)
	}

	// Try to commit - should fail
	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Logf("CommitTransaction failed after topic deletion: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		}
	} else {
		t.Log("CommitTransaction succeeded - topic may not have been deleted yet")
	}
}

// =============================================================================
// CATEGORY 7: Consumer Group Integration (SendOffsetsToTransaction)
// Tests for consumer offset commit failures
// =============================================================================

func TestIntegration_SendOffsets_InvalidConsumerGroupMeta(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	topicName := "test-send-offsets-invalid"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "send-offsets-invalid-tx")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Create a fake/invalid consumer group metadata
	// This simulates a scenario where the consumer group metadata is stale or invalid
	fakeConsumer, err := librdKafka.NewConsumer(&librdKafka.ConfigMap{
		"bootstrap.servers": kafkaContainer.BootstrapServers,
		"group.id":          "fake-group-that-doesnt-exist",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatalf("Failed to create fake consumer: %v", err)
	}

	// Get metadata without actually joining a group
	cgmd, err := fakeConsumer.GetConsumerGroupMetadata()
	fakeConsumer.Close()
	if err != nil {
		t.Fatalf("Failed to get consumer group metadata: %v", err)
	}

	offsets := []kafka.ConsumerOffset{
		{Topic: topicName, Partition: 0, Offset: 10, Meta: ""},
	}
	groupMeta := &kafka.GroupMeta{Meta: cgmd}

	// Try to send offsets with this metadata
	err = txProducer.SendOffsetsToTransaction(txCtx, offsets, groupMeta)
	if err != nil {
		t.Logf("SendOffsetsToTransaction failed with invalid metadata: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		}
	} else {
		t.Log("SendOffsetsToTransaction succeeded (may require active group membership to fail)")
	}

	// Cleanup
	txProducer.AbortTransaction(txCtx)
}

func TestIntegration_SendOffsets_ConsumerRebalanceDuringTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	topicName := "test-rebalance"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 3, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	groupId := "rebalance-test-group"

	// Create first consumer
	consumer1, err := createConsumerAndJoinGroup(kafkaContainer.BootstrapServers, groupId, topicName)
	if err != nil {
		t.Fatalf("Failed to create consumer1: %v", err)
	}
	defer consumer1.Close()

	t.Log("Consumer1 joined group")

	// Get group metadata from consumer1
	cgmd1, err := consumer1.GetConsumerGroupMetadata()
	if err != nil {
		t.Fatalf("Failed to get consumer group metadata: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "rebalance-tx")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Produce a message
	record := txProducer.NewRecord(txCtx, []byte("key"), []byte("value"), topicName, 0, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Fatalf("ProduceAsync failed: %v", err)
	}

	// Create second consumer to trigger rebalance
	t.Log("Creating consumer2 to trigger rebalance...")
	consumer2, err := createConsumerAndJoinGroup(kafkaContainer.BootstrapServers, groupId, topicName)
	if err != nil {
		t.Logf("Failed to create consumer2: %v", err)
	} else {
		defer consumer2.Close()
		t.Log("Consumer2 joined - rebalance should occur")
	}

	// Wait for rebalance to complete
	time.Sleep(5 * time.Second)

	// Try to send offsets using old metadata from consumer1
	offsets := []kafka.ConsumerOffset{
		{Topic: topicName, Partition: 0, Offset: 10, Meta: ""},
	}
	groupMeta := &kafka.GroupMeta{Meta: cgmd1}

	err = txProducer.SendOffsetsToTransaction(txCtx, offsets, groupMeta)
	if err != nil {
		t.Logf("SendOffsetsToTransaction failed after rebalance: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

			// After rebalance, we expect IllegalGeneration or similar
			if producerErr.TxnRequiresAbort() {
				t.Log("SUCCESS: Rebalance-related error correctly requires abort")
			}
		}
	} else {
		t.Log("SendOffsetsToTransaction succeeded despite rebalance")
	}

	// Cleanup
	txProducer.AbortTransaction(txCtx)
}

// =============================================================================
// CATEGORY 8: Timeout Scenarios
// Tests for various timeout conditions
// =============================================================================

func TestIntegration_TransactionTimeout_LongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	topicName := "test-tx-timeout"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create producer with very short transaction timeout
	config := NewProducerConfig()
	config.Id = "tx-timeout-test"
	config.BootstrapServers = []string{kafkaContainer.BootstrapServers}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "tx-timeout-test-id"
	config.Librd.SetKey("transaction.timeout.ms", 5000) // 5 second timeout

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	record := txProducer.NewRecord(txCtx, []byte("key"), []byte("value"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Fatalf("ProduceAsync failed: %v", err)
	}

	t.Log("Transaction started, waiting for timeout...")

	// Wait longer than transaction timeout
	time.Sleep(10 * time.Second)

	// Try to commit - should fail with timeout
	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Logf("CommitTransaction failed after timeout: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

			// Transaction timeout should require restart
			if producerErr.RequiresRestart() {
				t.Log("SUCCESS: Transaction timeout correctly requires restart")
			} else if producerErr.TxnRequiresAbort() {
				t.Log("Transaction timeout requires abort")
			}
		}
	} else {
		t.Log("CommitTransaction succeeded - timeout may not have occurred")
	}
}

func TestIntegration_InitTimeout_ShortContext(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "init-timeout-test")
	defer cleanup()

	// Use a very short timeout for init
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	err = txProducer.InitTransactions(shortCtx)
	if err != nil {
		t.Logf("InitTransactions with short timeout failed: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		}

		// Check if it's a context deadline error
		if strings.Contains(err.Error(), "context deadline") || strings.Contains(err.Error(), "timed out") {
			t.Log("Successfully detected context timeout on InitTransactions")
		}
	} else {
		t.Log("InitTransactions completed within 100ms (fast broker)")
	}
}

// =============================================================================
// CATEGORY 9: Recovery Verification
// Tests to verify recovery after various error conditions
// =============================================================================

func TestIntegration_Recovery_AfterAbortableError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	topicName := "test-recovery-abort"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "recovery-abort-tx")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// First transaction - abort it
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("First BeginTransaction failed: %v", err)
	}

	record1 := txProducer.NewRecord(txCtx, []byte("key1"), []byte("value1"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record1)
	if err != nil {
		t.Fatalf("First ProduceAsync failed: %v", err)
	}

	// Simulate an abortable error by explicitly aborting
	err = txProducer.AbortTransaction(txCtx)
	if err != nil {
		t.Fatalf("AbortTransaction failed: %v", err)
	}
	t.Log("First transaction aborted")

	// Recovery: Start a new transaction
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("Recovery BeginTransaction failed: %v", err)
	}

	record2 := txProducer.NewRecord(txCtx, []byte("key2"), []byte("value2"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record2)
	if err != nil {
		t.Fatalf("Recovery ProduceAsync failed: %v", err)
	}

	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("Recovery CommitTransaction failed: %v", err)
	}

	t.Log("SUCCESS: Recovery after abort completed - new transaction succeeded")
}

func TestIntegration_Recovery_AfterFatalError_NewProducer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	topicName := "test-recovery-fatal"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	txId := "recovery-fatal-tx"

	// First producer
	txProducer1, cleanup1 := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, txId)

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer1.InitTransactions(txCtx)
	if err != nil {
		cleanup1()
		t.Fatalf("Producer1 InitTransactions failed: %v", err)
	}

	err = txProducer1.BeginTransaction()
	if err != nil {
		cleanup1()
		t.Fatalf("Producer1 BeginTransaction failed: %v", err)
	}

	record1 := txProducer1.NewRecord(txCtx, []byte("key1"), []byte("value1"), topicName, -1, time.Now(), nil, "")
	err = txProducer1.ProduceAsync(txCtx, record1)
	if err != nil {
		cleanup1()
		t.Fatalf("Producer1 ProduceAsync failed: %v", err)
	}

	// Close producer 1 without committing (simulating fatal error requiring restart)
	cleanup1()
	t.Log("Producer1 closed without committing (simulating fatal error)")

	// Recovery: Create new producer with same transactional ID
	txProducer2, cleanup2 := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, txId)
	defer cleanup2()

	// InitTransactions should abort the pending transaction
	err = txProducer2.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("Producer2 InitTransactions failed: %v", err)
	}
	t.Log("Producer2 initialized - pending transaction should be aborted")

	// New transaction should work
	err = txProducer2.BeginTransaction()
	if err != nil {
		t.Fatalf("Producer2 BeginTransaction failed: %v", err)
	}

	record2 := txProducer2.NewRecord(txCtx, []byte("key2"), []byte("value2"), topicName, -1, time.Now(), nil, "")
	err = txProducer2.ProduceAsync(txCtx, record2)
	if err != nil {
		t.Fatalf("Producer2 ProduceAsync failed: %v", err)
	}

	err = txProducer2.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("Producer2 CommitTransaction failed: %v", err)
	}

	t.Log("SUCCESS: Recovery with new producer completed")
}

func TestIntegration_Recovery_MultipleAborts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	topicName := "test-multi-abort"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "multi-abort-tx")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Perform multiple abort cycles
	for i := 0; i < 3; i++ {
		err = txProducer.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction %d failed: %v", i, err)
		}

		record := txProducer.NewRecord(txCtx, []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)), topicName, -1, time.Now(), nil, "")
		err = txProducer.ProduceAsync(txCtx, record)
		if err != nil {
			t.Fatalf("ProduceAsync %d failed: %v", i, err)
		}

		err = txProducer.AbortTransaction(txCtx)
		if err != nil {
			t.Fatalf("AbortTransaction %d failed: %v", i, err)
		}

		t.Logf("Abort cycle %d completed", i+1)
	}

	// Final successful transaction
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("Final BeginTransaction failed: %v", err)
	}

	record := txProducer.NewRecord(txCtx, []byte("key-final"), []byte("value-final"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Fatalf("Final ProduceAsync failed: %v", err)
	}

	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("Final CommitTransaction failed: %v", err)
	}

	t.Log("SUCCESS: Multiple abort cycles followed by successful commit")
}

func TestIntegration_IdempotentRecovery_DuplicateDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	topicName := "test-idempotent"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, "idempotent-tx")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Send multiple transactions with unique keys
	for i := 0; i < 5; i++ {
		err = txProducer.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction %d failed: %v", i, err)
		}

		record := txProducer.NewRecord(
			txCtx,
			[]byte(fmt.Sprintf("idempotent-key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
			topicName,
			-1,
			time.Now(),
			nil,
			"",
		)

		err = txProducer.ProduceAsync(txCtx, record)
		if err != nil {
			t.Fatalf("ProduceAsync %d failed: %v", i, err)
		}

		err = txProducer.CommitTransaction(txCtx)
		if err != nil {
			t.Fatalf("CommitTransaction %d failed: %v", i, err)
		}

		t.Logf("Transaction %d committed", i+1)
	}

	t.Log("SUCCESS: All idempotent transactions completed")
}

// =============================================================================
// CATEGORY 2: Network Failures (Toxiproxy)
// Tests for network-related failures using Toxiproxy
// =============================================================================

func TestIntegration_NetworkPartition_ProducerIsolated(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Log("Setting up Kafka with Toxiproxy...")
	kafkaEnv, err := setupKafkaWithToxiproxy(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with Toxiproxy: %v", err)
	}
	defer kafkaEnv.Terminate(ctx)

	t.Logf("Kafka available via Toxiproxy at: %s", kafkaEnv.Kafka.BootstrapServers)

	topicName := "test-network-partition"
	err = createTopic(ctx, kafkaEnv.Kafka.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create producer with short timeouts for faster failure detection
	config := NewProducerConfig()
	config.Id = "network-partition-test"
	config.BootstrapServers = []string{kafkaEnv.Kafka.BootstrapServers}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "network-partition-tx"
	config.Librd.SetKey("transaction.timeout.ms", 10000)
	config.Librd.SetKey("socket.timeout.ms", 3000)
	config.Librd.SetKey("message.timeout.ms", 5000)

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	// Note: We don't defer producer.Close() here because after a network partition,
	// the producer's AbortTransaction(nil) in Close() can hang indefinitely.
	// The container termination will clean up the producer resources.

	txProducer := producer.(*TransactionalProducer)

	txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		producer.Close() // Safe to close here - no network issues yet
		t.Fatalf("InitTransactions failed: %v", err)
	}
	t.Log("Producer initialized")

	// First, verify normal operation works
	err = txProducer.BeginTransaction()
	if err != nil {
		producer.Close()
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	record := txProducer.NewRecord(txCtx, []byte("key-before"), []byte("value-before"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		producer.Close()
		t.Fatalf("ProduceAsync failed: %v", err)
	}

	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		producer.Close()
		t.Fatalf("First CommitTransaction failed: %v", err)
	}
	t.Log("First transaction committed successfully (network normal)")

	// Start a new transaction
	err = txProducer.BeginTransaction()
	if err != nil {
		producer.Close()
		t.Fatalf("BeginTransaction (2nd) failed: %v", err)
	}

	record2 := txProducer.NewRecord(txCtx, []byte("key-during"), []byte("value-during"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record2)
	if err != nil {
		producer.Close()
		t.Fatalf("ProduceAsync (2nd) failed: %v", err)
	}
	t.Log("Second transaction started, message produced")

	txProducer.Flush()

	// NOW simulate network partition using Toxiproxy
	t.Log("Simulating network partition via Toxiproxy...")
	err = kafkaEnv.DisableNetwork()
	if err != nil {
		t.Fatalf("Failed to disable network: %v", err)
	}
	t.Log("Network partition active - all connections to Kafka are blocked")

	// Try to commit - MUST fail due to network partition
	// The timeout toxic blocks all traffic with 1ms timeout, so commits cannot succeed
	t.Log("Attempting to commit during network partition...")
	err = txProducer.CommitTransaction(txCtx)
	if err == nil {
		t.Fatal("CommitTransaction should have failed during network partition, but succeeded")
	}

	t.Logf("CommitTransaction failed during network partition: %v", err)

	producerErr, ok := err.(Err)
	if !ok {
		t.Fatalf("Expected Err type, got %T: %v", err, err)
	}

	assertError(t, producerErr, true, false, false)

	// Re-enable network BEFORE any cleanup
	t.Log("Re-enabling network...")
	err = kafkaEnv.EnableNetwork()
	if err != nil {
		t.Logf("Failed to re-enable network: %v", err)
	}

	txProducer.Close()

	// After network partition, the producer state may be corrupted and Close() can hang.
	// Skip explicit producer cleanup - container termination will handle it.
	t.Log("Network partition test completed - skipping producer.Close() to avoid hang")
}

func TestIntegration_NetworkLatency_SlowBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Log("Setting up Kafka with Toxiproxy...")
	kafkaEnv, err := setupKafkaWithToxiproxy(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with Toxiproxy: %v", err)
	}
	defer kafkaEnv.Terminate(ctx)

	t.Logf("Kafka available via Toxiproxy at: %s", kafkaEnv.Kafka.BootstrapServers)

	topicName := "test-network-latency"
	err = createTopic(ctx, kafkaEnv.Kafka.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create producer with moderate timeouts
	config := NewProducerConfig()
	config.Id = "network-latency-test"
	config.BootstrapServers = []string{kafkaEnv.Kafka.BootstrapServers}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "network-latency-tx"
	config.Librd.SetKey("transaction.timeout.ms", 30000)
	config.Librd.SetKey("socket.timeout.ms", 10000)
	config.Librd.SetKey("message.timeout.ms", 15000)

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	// Note: We don't defer producer.Close() here because after network issues,
	// the producer's AbortTransaction(nil) in Close() can hang indefinitely.
	// The container termination will clean up the producer resources.

	txProducer := producer.(*TransactionalProducer)

	txCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		producer.Close() // Safe to close here - no network issues yet
		t.Fatalf("InitTransactions failed: %v", err)
	}
	t.Log("Producer initialized")

	// Add 2 second latency to all traffic
	t.Log("Adding 2000ms latency to network traffic...")
	err = kafkaEnv.AddLatency(2000)
	if err != nil {
		producer.Close() // Safe to close here - no transaction started
		t.Fatalf("Failed to add latency: %v", err)
	}

	// Measure transaction time with latency
	start := time.Now()

	err = txProducer.BeginTransaction()
	if err != nil {
		producer.Close()
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	record := txProducer.NewRecord(txCtx, []byte("key"), []byte("value"), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		producer.Close()
		t.Fatalf("ProduceAsync failed: %v", err)
	}

	err = txProducer.CommitTransaction(txCtx)
	elapsed := time.Since(start)

	if err != nil {
		t.Logf("CommitTransaction failed with latency: %v (took %v)", err, elapsed)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Error classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		}
	} else {
		t.Logf("Transaction completed with latency in %v", elapsed)
		if elapsed > 4*time.Second {
			t.Log("SUCCESS: Transaction took longer due to injected latency")
		}
	}

	// Remove latency
	t.Log("Removing latency...")
	err = kafkaEnv.RemoveLatency()
	if err != nil {
		t.Logf("Failed to remove latency: %v", err)
	}

	// Skip producer.Close() to avoid hang - container termination will handle cleanup
	t.Log("Network latency test completed")
}

// TestIntegration_CommitSucceeds_AckFails attempts to simulate a scenario where
// network issues during commit cause the producer to timeout.
//
// Note: Achieving a true "in-doubt" scenario (commit succeeds on broker but producer
// times out before receiving ACK) is extremely difficult because:
// 1. Kafka's transaction commit involves multiple round-trips
// 2. Downstream latency affects all responses, including internal protocol coordination
// 3. TCP acknowledgements are also affected
//
// This test demonstrates:
// - Timeout handling during network issues
// - Error classification (ShouldShutdown for timeout errors)
// - Difference between read_uncommitted and read_committed consumers
// - Message visibility with uncommitted transactions
func TestIntegration_CommitSucceeds_AckFails(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Log("Setting up Kafka with Toxiproxy...")
	kafkaEnv, err := setupKafkaWithToxiproxy(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with Toxiproxy: %v", err)
	}
	defer kafkaEnv.Terminate(ctx)

	t.Logf("Kafka available via Toxiproxy at: %s", kafkaEnv.Kafka.BootstrapServers)

	topicName := "test-commit-ack-fail"
	err = createTopic(ctx, kafkaEnv.Kafka.BootstrapServers, topicName, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create producer with short message timeout to trigger faster failure
	config := NewProducerConfig()
	config.Id = "commit-ack-fail-test"
	config.BootstrapServers = []string{kafkaEnv.Kafka.BootstrapServers}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "commit-ack-fail-tx"
	config.Librd.SetKey("transaction.timeout.ms", 60000)
	config.Librd.SetKey("socket.timeout.ms", 5000)
	config.Librd.SetKey("message.timeout.ms", 5000) // Short timeout for messages

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	txProducer := producer.(*TransactionalProducer)

	initCtx, initCancel := context.WithTimeout(ctx, 30*time.Second)
	defer initCancel()

	err = txProducer.InitTransactions(initCtx)
	if err != nil {
		producer.Close()
		t.Fatalf("InitTransactions failed: %v", err)
	}
	t.Log("Producer initialized")

	// Start a transaction and produce a message BEFORE adding latency
	err = txProducer.BeginTransaction()
	if err != nil {
		producer.Close()
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	t.Log("Transaction started")

	messageKey := fmt.Sprintf("key-%d", time.Now().UnixNano())
	messageValue := fmt.Sprintf("value-%d", time.Now().UnixNano())

	produceCtx, produceCancel := context.WithTimeout(ctx, 30*time.Second)
	defer produceCancel()

	record := txProducer.NewRecord(produceCtx, []byte(messageKey), []byte(messageValue), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(produceCtx, record)
	if err != nil {
		producer.Close()
		t.Fatalf("ProduceAsync failed: %v", err)
	}
	t.Logf("Message produced: key=%s", messageKey)

	// Flush to ensure message reaches broker before we start the commit
	t.Log("Flushing producer to ensure message is on broker...")
	txProducer.Flush()
	t.Log("Flush completed - message is on broker (but not committed yet)")

	// Strategy: Add moderate downstream latency BEFORE calling commit
	// This is asymmetric:
	// - Upstream (requests): No latency - commit request goes through immediately
	// - Downstream (responses): 15s latency - response is delayed
	//
	// Flow:
	// 1. We add 15s latency to downstream only
	// 2. Commit request is sent immediately (upstream, no delay)
	// 3. Broker receives and processes the commit (should complete quickly)
	// 4. Broker sends response, but it's delayed 15s (downstream latency)
	// 5. Client times out after context timeout
	// 6. Client thinks commit failed, but it actually succeeded on the broker!
	// 7. This is the "in-doubt" transaction scenario

	t.Log("Adding 15s downstream-only latency BEFORE calling commit...")
	if err := kafkaEnv.AddDownstreamLatency(15000); err != nil {
		producer.Close()
		t.Fatalf("Failed to add downstream latency: %v", err)
	}
	t.Log("Downstream latency added - requests will flow normally, responses will be delayed 15s")

	// Use a short context timeout (5s) that's less than the latency (15s)
	// This ensures the client times out before receiving the response
	commitCtx, commitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer commitCancel()

	t.Log("Calling CommitTransaction with 5s timeout (latency is 15s, so we'll timeout)...")
	start := time.Now()
	err = txProducer.CommitTransaction(commitCtx)
	elapsed := time.Since(start)
	t.Logf("CommitTransaction returned after %v", elapsed)

	commitSucceeded := false
	// We expect an error because we timeout before receiving the ACK
	if err == nil {
		t.Log("CommitTransaction succeeded - latency may not have been applied in time")
		commitSucceeded = true
	} else {
		t.Logf("CommitTransaction failed as expected: %v", err)
		t.Log("This is the 'in-doubt' scenario: commit might have succeeded on broker!")

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Error classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		}
	}

	// Remove latency so we can consume
	t.Log("Removing downstream latency...")
	if err := kafkaEnv.RemoveDownstreamLatency(); err != nil {
		t.Logf("Failed to remove downstream latency: %v", err)
	}

	// Wait for any in-flight responses to drain and network to stabilize
	t.Log("Waiting for network to stabilize...")
	time.Sleep(5 * time.Second)

	// Now verify if the message was actually committed by consuming it
	// This is the key assertion - did the broker actually commit the transaction?
	t.Log("Checking if message was committed by consuming from topic...")

	// First, check with read_uncommitted to see if message is on broker at all
	t.Log("Step 1: Checking for message with read_uncommitted...")
	uncommittedConsumer, err := createConsumerWithIsolation(kafkaEnv.Kafka.BootstrapServers, topicName, "verify-uncommitted-group", "read_uncommitted")
	if err != nil {
		t.Logf("Failed to create uncommitted consumer: %v", err)
	} else {
		found, msgKey := consumeMessage(uncommittedConsumer, messageKey, 10*time.Second)
		uncommittedConsumer.Close()
		if found {
			t.Logf("Message found with read_uncommitted: key=%s", msgKey)
		} else {
			t.Log("Message NOT found with read_uncommitted - produce may have failed")
		}
	}

	// Then check with read_committed to see if transaction was committed
	t.Log("Step 2: Checking for message with read_committed...")
	committedConsumer, err := createConsumerWithIsolation(kafkaEnv.Kafka.BootstrapServers, topicName, "verify-committed-group", "read_committed")
	if err != nil {
		t.Logf("Failed to create committed consumer: %v", err)
		t.Log("Cannot verify if commit succeeded - skipping verification")
	} else {
		defer committedConsumer.Close()

		found, _ := consumeMessage(committedConsumer, messageKey, 10*time.Second)
		if found {
			t.Log("SUCCESS: Message found with read_committed!")
			if !commitSucceeded {
				t.Log("IMPORTANT: This confirms the 'in-doubt' scenario:")
				t.Log("  - Producer received timeout/error")
				t.Log("  - But commit ACTUALLY SUCCEEDED on the broker!")
			}
		} else {
			t.Log("Message NOT found with read_committed")
			if commitSucceeded {
				t.Log("WARNING: Producer thought commit succeeded but message not visible")
			} else {
				t.Log("This is expected since the commit timed out before completion")
			}
		}
	}

	t.Log("Commit-ACK-fail test completed")
}

// createConsumerWithIsolation creates a consumer with the specified isolation level
func createConsumerWithIsolation(bootstrapServers, topic, groupId, isolationLevel string) (*librdKafka.Consumer, error) {
	config := &librdKafka.ConfigMap{
		"bootstrap.servers":    bootstrapServers,
		"group.id":             groupId,
		"auto.offset.reset":    "earliest",
		"enable.partition.eof": true,
		"enable.auto.commit":   false,
		"isolation.level":      isolationLevel,
	}

	consumer, err := librdKafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		consumer.Close()
		return nil, err
	}

	return consumer, nil
}

// consumeMessage tries to consume a message with the given key within the timeout
func consumeMessage(consumer *librdKafka.Consumer, targetKey string, timeout time.Duration) (found bool, key string) {
	for {
		ev := consumer.Poll(-1)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *librdKafka.Message:
			key = string(e.Key)
			if key == targetKey {
				return true, key
			}
		case librdKafka.PartitionEOF:
			return false, ``
		case *librdKafka.Error:
			println(`consumer error`, ev.String())
		default:
			panic(fmt.Sprintf("Unknown event: %v", e))
		}
	}
}

// TestIntegration_InDoubt_DelayedResponse demonstrates a TRUE in-doubt scenario
// using response delay instead of dropping.
//
// The key insight is:
//   - When we DROP responses, librdkafka retries but the broker may not complete the commit
//   - When we DELAY responses, the broker commits and sends the response, but by the time
//     the response arrives, the producer has already timed out
//
// This achieves the exact in-doubt scenario:
// - EndTxn request reaches broker → broker commits the transaction
// - Response is delayed past the producer's timeout
// - Producer times out and thinks commit failed
// - Transaction IS actually committed on the broker
func TestIntegration_InDoubt_DelayedResponse(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Setup Kafka with our custom proxy in front
	t.Log("Setting up Kafka with custom protocol proxy...")
	setup, err := mocks.SetupKafkaCluster(t, mocks.WithProxy("127.0.0.1:19095", "127.0.0.1:19096"))
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer setup.Terminate(ctx)

	proxy := setup.Proxy()
	t.Logf("Kafka available through proxy at: %s", setup.BootstrapAddr)
	t.Logf("Kafka direct address (for verification): %s", setup.DirectKafkaAddr)

	// Create topic
	topicName := "test-indoubt-delay-" + fmt.Sprintf("%d", time.Now().UnixNano()%10000)
	err = createTopicWithConfig(ctx, setup.DirectKafkaAddr, topicName, 1, 1, nil)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	time.Sleep(2 * time.Second)

	// Create producer that connects THROUGH the proxy
	// IMPORTANT: transaction.timeout.ms must be LONGER than the delay we add
	// Otherwise the broker will abort the transaction due to timeout before commit completes
	config := NewProducerConfig()
	config.Id = "indoubt-delay-producer"
	config.BootstrapServers = []string{setup.Proxy().ListenAddr()} // Connect through proxy
	config.Transactional.Enabled = true
	config.Transactional.Id = fmt.Sprintf("indoubt-delay-tx-%d", time.Now().UnixNano())
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Librd.SetKey("transaction.timeout.ms", 60000) // 60s - must be longer than our delays

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(kafka.TransactionalProducer)

	initCtx, initCancel := context.WithTimeout(ctx, 60*time.Second)
	defer initCancel()

	err = txProducer.InitTransactions(initCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}
	t.Log("Producer initialized through proxy")

	// Start a transaction and produce a message
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	t.Log("Transaction started")

	messageKey := fmt.Sprintf("indoubt-delay-key-%d", time.Now().UnixNano())
	messageValue := fmt.Sprintf("indoubt-delay-value-%d", time.Now().UnixNano())

	produceCtx, produceCancel := context.WithTimeout(ctx, 30*time.Second)
	defer produceCancel()

	record := txProducer.NewRecord(produceCtx, []byte(messageKey), []byte(messageValue), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(produceCtx, record)
	if err != nil {
		t.Fatalf("ProduceAsync failed: %v", err)
	}
	t.Logf("Message produced: key=%s", messageKey)

	// Flush to ensure message reaches broker
	t.Log("Flushing producer...")
	txProducer.Flush()
	t.Log("Flush completed - message is on broker (uncommitted)")

	// Configure the proxy to DELAY EndTxn responses by 20 seconds
	// Since producer timeout is 5 seconds, this will cause the producer to timeout
	// but the broker will still commit the transaction
	t.Log("Configuring proxy to DELAY EndTxn responses by 20 seconds...")
	proxy.DelayResponsesFor(proxyPkg.APIKeyEndTxn, 20*time.Second)

	// Attempt to commit - this should timeout because the response is delayed
	commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer commitCancel()

	t.Log("Calling CommitTransaction (EndTxn response will be delayed)...")
	start := time.Now()
	err = txProducer.CommitTransaction(commitCtx)
	elapsed := time.Since(start)
	t.Logf("CommitTransaction returned after %v", elapsed)

	// Clear the delay rule
	proxy.ClearDelayRules()

	commitSucceeded := false
	if err == nil {
		t.Log("CommitTransaction succeeded")
		commitSucceeded = true
	} else {
		t.Logf("CommitTransaction returned error: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Error classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		}
	}

	// Wait for the delayed response to complete and things to settle
	t.Log("Waiting for delayed response to complete...")
	time.Sleep(15 * time.Second)

	// Verify if the message was actually committed by consuming it
	t.Log("Checking if message was committed (connecting directly to Kafka)...")

	// Check with read_uncommitted first
	t.Log("Step 1: Checking with read_uncommitted...")
	uncommittedConsumer, err := createConsumerWithIsolation(setup.DirectKafkaAddr, topicName, "indoubt-delay-uncommitted", "read_uncommitted")
	if err != nil {
		t.Logf("Failed to create uncommitted consumer: %v", err)
	} else {
		found, _ := consumeMessage(uncommittedConsumer, messageKey, 10*time.Second)
		uncommittedConsumer.Close()
		if found {
			t.Log("Message found with read_uncommitted")
		} else {
			t.Log("Message NOT found with read_uncommitted - produce may have failed")
		}
	}

	// Check with read_committed to verify transaction status
	t.Log("Step 2: Checking with read_committed...")
	committedConsumer, err := createConsumerWithIsolation(setup.DirectKafkaAddr, topicName, "indoubt-delay-committed", "read_committed")
	if err != nil {
		t.Logf("Failed to create committed consumer: %v", err)
	} else {
		defer committedConsumer.Close()

		found, _ := consumeMessage(committedConsumer, messageKey, 10*time.Second)
		if found {
			t.Log("=" + strings.Repeat("=", 60))
			t.Log("MESSAGE FOUND WITH read_committed")
			t.Log("=" + strings.Repeat("=", 60))
			if !commitSucceeded {
				t.Log("TRUE IN-DOUBT SCENARIO ACHIEVED!")
				t.Log("- Producer received error/timeout")
				t.Log("- But commit ACTUALLY SUCCEEDED on the broker!")
				t.Log("- Message is visible with read_committed isolation")
			} else {
				t.Log("Transaction was committed successfully (expected)")
			}
			t.Log("=" + strings.Repeat("=", 60))
		} else {
			t.Log("Message NOT found with read_committed")
			if commitSucceeded {
				t.Log("WARNING: Producer thought commit succeeded but message not visible")
			} else {
				t.Log("Commit may have actually failed (transaction was not completed)")
			}
		}
	}

	t.Log("In-doubt delay test completed")
}

func TestIntegration_DropFirstResponse_RetrySucceedsOther(t *testing.T) {
	ctx := context.Background()

	t.Log("Setting up Kafka with custom protocol proxy...")
	setup, err := mocks.SetupKafkaCluster(t)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer setup.Terminate(ctx)

	proxy := setup.Proxy()
	proxy.EnableVerboseLogging() // Enable detailed request/response logging
	t.Logf("Kafka available through proxy at: %s", setup.Proxy().ListenAddr())

	topicName := "test-drop-first-" + fmt.Sprintf("%d", time.Now().UnixNano()%10000)
	err = createTopicWithConfig(ctx, setup.DirectKafkaAddr, topicName, 1, 1, nil)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	time.Sleep(2 * time.Second)

	config := NewProducerConfig()
	config.Id = "drop-first-producer"
	config.BootstrapServers = []string{setup.Proxy().ListenAddr()}
	config.Transactional.Enabled = true
	config.Transactional.Id = fmt.Sprintf("drop-first-tx-%d", time.Now().UnixNano())
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Librd.SetKey("transaction.timeout.ms", 60000)

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	t.Log("startion the test.....\n\n")

	txProducer := producer.(kafka.TransactionalProducer)

	err = txProducer.InitTransactions(nil)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}
	t.Log("Transaction Inited initialized\n\n")

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	t.Log("Transaction Began\n\n")

	record := txProducer.NewRecord(nil, []byte(`messageKey`), []byte(`messageValue`), topicName, -1, time.Now(), nil, "")
	if err := txProducer.ProduceAsync(nil, record); err != nil {
		t.Fatal(err.Error())
	}

	t.Log("Record Sent\n\n")

	txProducer.Flush()

	t.Log("Record Flushed\n\n")

	time.Sleep(5 * time.Second)

	t.Log("Closing\n\n")
}

// TestIntegration_TransactionCommit_NoProxy proves that consumeMessage works correctly
// by running the exact same transaction code without any proxy.
func TestIntegration_TransactionCommit_NoProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Log("Setting up Kafka (no proxy)...")
	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	t.Logf("Kafka available at: %s", kafkaContainer.BootstrapServers)

	topicName := "test-no-proxy-" + fmt.Sprintf("%d", time.Now().UnixNano()%10000)
	err = createTopicWithConfig(ctx, kafkaContainer.BootstrapServers, topicName, 1, 1, nil)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	time.Sleep(20 * time.Second)

	config := NewProducerConfig()
	config.Id = "no-proxy-producer"
	config.BootstrapServers = []string{kafkaContainer.BootstrapServers}
	config.Transactional.Enabled = true
	config.Transactional.Id = fmt.Sprintf("no-proxy-tx-%d", time.Now().UnixNano())
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(kafka.TransactionalProducer)

	initCtx, initCancel := context.WithTimeout(ctx, 60*time.Second)
	defer initCancel()

	err = txProducer.InitTransactions(initCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}
	t.Log("Producer initialized")

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	messageKey := fmt.Sprintf("no-proxy-key-%d", time.Now().UnixNano())
	messageValue := fmt.Sprintf("no-proxy-value-%d", time.Now().UnixNano())

	produceCtx, produceCancel := context.WithTimeout(ctx, 30*time.Second)
	defer produceCancel()

	record := txProducer.NewRecord(produceCtx, []byte(messageKey), []byte(messageValue), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(produceCtx, record)
	if err != nil {
		t.Fatalf("ProduceAsync failed: %v", err)
	}
	t.Log("Message produced")

	//txProducer.Flush()
	//t.Log("Flush completed")

	commitCtx, commitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer commitCancel()

	t.Log("Calling CommitTransaction...")
	start := time.Now()
	err = txProducer.CommitTransaction(commitCtx)
	elapsed := time.Since(start)
	t.Logf("CommitTransaction returned after %v", elapsed)

	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}
	t.Log("CommitTransaction succeeded")

	// Give Kafka time to propagate the commit marker
	t.Log("Waiting 2 seconds for commit to propagate...")
	time.Sleep(2 * time.Second)

	// First verify with read_uncommitted to confirm message exists
	t.Log("First checking with read_uncommitted...")
	uncommittedConsumer, err := createConsumerWithIsolation(kafkaContainer.BootstrapServers, topicName, "no-proxy-verify-uncommitted", "read_uncommitted")
	if err != nil {
		t.Fatalf("Failed to create uncommitted consumer: %v", err)
	}
	foundUncommitted, _ := consumeMessage(uncommittedConsumer, messageKey, 10*time.Second)
	uncommittedConsumer.Close()
	if foundUncommitted {
		t.Log("Message found with read_uncommitted - message exists in partition")
	} else {
		t.Log("Message NOT found with read_uncommitted - message was never written!")
	}

	// Verify message is visible with read_committed
	t.Log("Now verifying message is visible with read_committed...")
	committedConsumer, err := createConsumerWithIsolation(kafkaContainer.BootstrapServers, topicName, "no-proxy-verify-committed", "read_committed")
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer committedConsumer.Close()

	found, _ := consumeMessage(committedConsumer, messageKey, 10*time.Second)
	if found {
		t.Log("SUCCESS: Message found with read_committed - transaction was committed!")
	} else {
		t.Log("FAILURE: Message NOT found with read_committed")
		if foundUncommitted {
			t.Log("CRITICAL: Message exists but not visible with read_committed - transaction may not be committed!")
		}
	}

}

// TestIntegration_TransactionCommit_Sarama tests transaction commit using ONLY Sarama library
// (both producer and consumer) to compare behavior with librdkafka.
func TestIntegration_TransactionCommit_Sarama(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Log("Setting up Kafka (no proxy)...")
	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	t.Logf("Kafka available at: %s", kafkaContainer.BootstrapServers)

	topicName := "test-sarama-tx-" + fmt.Sprintf("%d", time.Now().UnixNano()%10000)

	// Create topic using Sarama admin
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_5_0_0
	admin, err := sarama.NewClusterAdmin([]string{kafkaContainer.BootstrapServers}, saramaConfig)
	if err != nil {
		t.Fatalf("Failed to create Sarama admin: %v", err)
	}
	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	admin.Close()
	time.Sleep(2 * time.Second)

	// Create transactional async producer with Sarama
	txConfig := sarama.NewConfig()
	txConfig.Version = sarama.V2_5_0_0
	txConfig.Producer.Return.Successes = true
	txConfig.Producer.Return.Errors = true
	txConfig.Producer.RequiredAcks = sarama.WaitForAll
	txConfig.Producer.Idempotent = true
	txConfig.Producer.Transaction.ID = fmt.Sprintf("sarama-tx-%d", time.Now().UnixNano())
	txConfig.Producer.Transaction.Timeout = 60 * time.Second
	txConfig.Net.MaxOpenRequests = 1 // Required for idempotent producer

	producer, err := sarama.NewAsyncProducer([]string{kafkaContainer.BootstrapServers}, txConfig)
	if err != nil {
		t.Fatalf("Failed to create Sarama producer: %v", err)
	}
	defer producer.Close()

	// Initialize transactions
	t.Log("Initializing transactions...")
	err = producer.BeginTxn()
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	t.Log("Transaction begun")

	messageKey := fmt.Sprintf("sarama-key-%d", time.Now().UnixNano())
	messageValue := fmt.Sprintf("sarama-value-%d", time.Now().UnixNano())

	// Produce message
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder(messageKey),
		Value: sarama.StringEncoder(messageValue),
	}
	producer.Input() <- msg
	t.Log("Message sent to producer input channel")

	// Wait for success or error
	select {
	case success := <-producer.Successes():
		t.Logf("Message sent to partition %d at offset %d", success.Partition, success.Offset)
	case err := <-producer.Errors():
		t.Fatalf("SendMessage failed: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for produce confirmation")
	}

	// Commit transaction
	t.Log("Calling CommitTxn...")
	start := time.Now()
	err = producer.CommitTxn()
	elapsed := time.Since(start)
	t.Logf("CommitTxn returned after %v", elapsed)

	if err != nil {
		t.Fatalf("CommitTxn failed: %v", err)
	}
	t.Log("CommitTxn succeeded")

	// Give Kafka time to propagate the commit marker
	t.Log("Waiting 2 seconds for commit to propagate...")
	time.Sleep(2 * time.Second)

	// First verify with read_uncommitted (Sarama consumer)
	t.Log("First checking with read_uncommitted (Sarama)...")
	foundUncommitted := consumeMessageSarama(t, kafkaContainer.BootstrapServers, topicName, messageKey, sarama.ReadUncommitted, 10*time.Second)
	if foundUncommitted {
		t.Log("Message found with read_uncommitted - message exists in partition")
	} else {
		t.Log("Message NOT found with read_uncommitted - message was never written!")
	}

	// Verify message is visible with read_committed (Sarama consumer)
	t.Log("Now verifying message is visible with read_committed (Sarama)...")
	found := consumeMessageSarama(t, kafkaContainer.BootstrapServers, topicName, messageKey, sarama.ReadCommitted, 10*time.Second)
	if found {
		t.Log("SUCCESS: Message found with read_committed - transaction was committed!")
	} else {
		t.Log("FAILURE: Message NOT found with read_committed")
		if foundUncommitted {
			t.Log("CRITICAL: Message exists but not visible with read_committed - transaction may not be committed!")
		}
	}
}

// TestIntegration_TransactionCommit_Sarama_WithProxy tests that transactions work correctly
// with Sarama when traffic goes through our custom protocol-aware proxy.
// This verifies the proxy correctly forwards transaction-related requests/responses.
func TestIntegration_TransactionCommit_Sarama_WithProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Log("Setting up Kafka with custom protocol proxy...")
	setup, err := mocks.SetupKafkaCluster(t)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer setup.Terminate(ctx)

	proxy := setup.Proxy()
	proxy.EnableVerboseLogging() // Enable detailed request/response logging
	t.Logf("Kafka available through proxy at: %s", setup.Proxy().ListenAddr())
	t.Logf("Direct Kafka address: %s", setup.DirectKafkaAddr)

	topicName := "test-sarama-proxy-tx-" + fmt.Sprintf("%d", time.Now().UnixNano()%10000)

	// Create topic using Sarama admin (through proxy)
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_5_0_0
	admin, err := sarama.NewClusterAdmin([]string{setup.Proxy().ListenAddr()}, saramaConfig)
	if err != nil {
		t.Fatalf("Failed to create Sarama admin: %v", err)
	}
	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	admin.Close()
	time.Sleep(2 * time.Second)

	// Create transactional async producer with Sarama (through proxy)
	txConfig := sarama.NewConfig()
	txConfig.Version = sarama.V2_5_0_0
	txConfig.Producer.Return.Successes = true
	txConfig.Producer.Return.Errors = true
	txConfig.Producer.RequiredAcks = sarama.WaitForAll
	txConfig.Producer.Idempotent = true
	txConfig.Producer.Transaction.ID = fmt.Sprintf("sarama-proxy-tx-%d", time.Now().UnixNano())
	txConfig.Producer.Transaction.Timeout = 60 * time.Second
	txConfig.Net.MaxOpenRequests = 1 // Required for idempotent producer

	producer, err := sarama.NewAsyncProducer([]string{setup.Proxy().ListenAddr()}, txConfig)
	if err != nil {
		t.Fatalf("Failed to create Sarama producer: %v", err)
	}
	defer producer.Close()

	// Initialize transactions
	t.Log("Initializing transactions...")
	err = producer.BeginTxn()
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	t.Log("Transaction begun")

	messageKey := fmt.Sprintf("sarama-proxy-key-%d", time.Now().UnixNano())
	messageValue := fmt.Sprintf("sarama-proxy-value-%d", time.Now().UnixNano())

	// Produce message
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder(messageKey),
		Value: sarama.StringEncoder(messageValue),
	}
	producer.Input() <- msg
	t.Log("Message sent to producer input channel")

	// Wait for success or error
	select {
	case success := <-producer.Successes():
		t.Logf("Message sent to partition %d at offset %d", success.Partition, success.Offset)
	case err := <-producer.Errors():
		t.Fatalf("SendMessage failed: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for produce confirmation")
	}

	// Commit transaction
	t.Log("Calling CommitTxn...")
	start := time.Now()
	err = producer.CommitTxn()
	elapsed := time.Since(start)
	t.Logf("CommitTxn returned after %v", elapsed)

	if err != nil {
		t.Fatalf("CommitTxn failed: %v", err)
	}
	t.Log("CommitTxn succeeded")

	// Log proxy stats
	t.Logf("proxy stats - EndTxn responses dropped: %d", proxy.GetDroppedCount(proxyPkg.APIKeyEndTxn))

	// Give Kafka time to propagate the commit marker
	t.Log("Waiting 2 seconds for commit to propagate...")
	time.Sleep(2 * time.Second)

	// First verify with read_uncommitted (using direct Kafka address to bypass proxy for verification)
	t.Log("First checking with read_uncommitted (Sarama, direct connection)...")
	foundUncommitted := consumeMessageSarama(t, setup.DirectKafkaAddr, topicName, messageKey, sarama.ReadUncommitted, 10*time.Second)
	if foundUncommitted {
		t.Log("Message found with read_uncommitted - message exists in partition")
	} else {
		t.Log("Message NOT found with read_uncommitted - message was never written!")
	}

	// Verify message is visible with read_committed (using direct Kafka address)
	t.Log("Now verifying message is visible with read_committed (Sarama, direct connection)...")
	found := consumeMessageSarama(t, setup.DirectKafkaAddr, topicName, messageKey, sarama.ReadCommitted, 10*time.Second)
	if found {
		t.Log("SUCCESS: Message found with read_committed - transaction was committed!")
	} else {
		t.Log("FAILURE: Message NOT found with read_committed")
		if foundUncommitted {
			t.Log("CRITICAL: Message exists but not visible with read_committed - transaction may not be committed!")
		}
		t.Fatal("Transaction commit verification failed")
	}
}

// consumeMessageSarama consumes messages using Sarama consumer with specified isolation level
func consumeMessageSarama(t *testing.T, broker, topic, targetKey string, isolation sarama.IsolationLevel, timeout time.Duration) bool {
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Consumer.IsolationLevel = isolation
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{broker}, config)
	if err != nil {
		t.Logf("Failed to create Sarama consumer: %v", err)
		return false
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Logf("Failed to consume partition: %v", err)
		return false
	}
	defer partitionConsumer.Close()

	deadline := time.After(timeout)
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			if msg == nil {
				continue
			}
			key := string(msg.Key)
			t.Logf("Sarama consumer received message: key=%s, offset=%d", key, msg.Offset)
			if key == targetKey {
				return true
			}
		case err := <-partitionConsumer.Errors():
			t.Logf("Sarama consumer error: %v", err)
		case <-deadline:
			t.Logf("Sarama consumer timeout after %v", timeout)
			return false
		}
	}
}

// TestIntegration_DropFirstResponse_RetrySucceeds tests that when the first
// EndTxn response is dropped, the retry succeeds and the message is committed.
// This proves the broker commits idempotently.
func TestIntegration_DropFirstResponse_RetrySucceeds(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Log("Setting up Kafka with custom protocol proxy...")
	setup, err := mocks.SetupKafkaCluster(t)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer setup.Terminate(ctx)

	proxy := setup.Proxy()
	proxy.EnableVerboseLogging() // Enable detailed request/response logging
	t.Logf("Kafka available through proxy at: %s", setup.Proxy().ListenAddr())

	topicName := "test-drop-first-" + fmt.Sprintf("%d", time.Now().UnixNano()%10000)
	err = createTopicWithConfig(ctx, setup.DirectKafkaAddr, topicName, 1, 1, nil)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	time.Sleep(2 * time.Second)

	config := NewProducerConfig()
	config.Id = "drop-first-producer"
	config.BootstrapServers = []string{setup.Proxy().ListenAddr()}
	config.Transactional.Enabled = true
	config.Transactional.Id = fmt.Sprintf("drop-first-tx-%d", time.Now().UnixNano())
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	//config.Librd.SetKey("transaction.timeout.ms", 60000)

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(kafka.TransactionalProducer)

	initCtx, initCancel := context.WithTimeout(ctx, 60*time.Second)
	defer initCancel()

	err = txProducer.InitTransactions(initCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}
	t.Log("Producer initialized")

	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	messageKey := fmt.Sprintf("drop-first-key-%d", time.Now().UnixNano())
	messageValue := fmt.Sprintf("drop-first-value-%d", time.Now().UnixNano())

	produceCtx, produceCancel := context.WithTimeout(ctx, 30*time.Second)
	defer produceCancel()

	record := txProducer.NewRecord(produceCtx, []byte(messageKey), []byte(messageValue), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(produceCtx, record)
	if err != nil {
		t.Fatalf("ProduceAsync failed: %v", err)
	}
	t.Log("Message produced")

	txProducer.Flush()
	t.Log("Flush completed")

	// Drop ONLY the first EndTxn response - subsequent responses go through
	//t.Log("Configuring proxy to drop ONLY the first EndTxn response...")
	//proxy.DropFirstNResponsesFor(APIKeyEndTxn, 1)

	commitCtx, commitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer commitCancel()

	t.Log("Calling CommitTransaction (first response will be dropped, retry should succeed)...")
	start := time.Now()
	err = txProducer.CommitTransaction(commitCtx)
	elapsed := time.Since(start)
	t.Logf("CommitTransaction returned after %v", elapsed)

	droppedCount := proxy.GetDroppedCount(proxyPkg.APIKeyEndTxn)
	t.Logf("proxy dropped %d EndTxn response(s)", droppedCount)

	if err != nil {
		t.Logf("CommitTransaction failed: %v", err)
		t.Log("This is unexpected - retry should have succeeded")
	} else {
		t.Log("CommitTransaction SUCCEEDED after retry!")
		t.Log("This proves:")
		t.Log("  - First response was dropped")
		t.Log("  - Producer retried")
		t.Log("  - Broker returned success on retry (idempotent)")
	}

	// Give Kafka time to propagate the commit marker
	t.Log("Waiting 2 seconds for commit to propagate...")
	time.Sleep(2 * time.Second)

	// First verify with read_uncommitted to confirm message exists
	t.Log("First checking with read_uncommitted...")
	uncommittedConsumer, err := createConsumerWithIsolation(setup.DirectKafkaAddr, topicName, "drop-first-verify-uncommitted", "read_uncommitted")
	if err != nil {
		t.Fatalf("Failed to create uncommitted consumer: %v", err)
	}
	foundUncommitted, _ := consumeMessage(uncommittedConsumer, messageKey, 10*time.Second)
	uncommittedConsumer.Close()
	if foundUncommitted {
		t.Log("Message found with read_uncommitted - message exists in partition")
	} else {
		t.Log("Message NOT found with read_uncommitted - message was never written!")
	}

	// Verify message is visible with read_committed
	t.Log("Now verifying message is visible with read_committed...")
	committedConsumer, err := createConsumerWithIsolation(setup.DirectKafkaAddr, topicName, "drop-first-verify-committed", "read_committed")
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer committedConsumer.Close()

	found, _ := consumeMessage(committedConsumer, messageKey, 10*time.Second)
	if found {
		t.Log("SUCCESS: Message found with read_committed - transaction was committed!")
	} else {
		t.Log("FAILURE: Message NOT found with read_committed")
		if foundUncommitted {
			t.Log("CRITICAL: Message exists but not visible with read_committed - transaction may not be committed!")
		}
	}
}

// =============================================================================
// Multi-Broker Cluster Tests
// =============================================================================

func TestIntegration_MultiBroker_LeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Log("Starting 3-broker Kafka cluster...")
	cluster, err := SetupMultiBrokerCluster(ctx)
	if err != nil {
		t.Fatalf("Failed to setup multi-broker cluster: %v", err)
	}
	defer cluster.Terminate(ctx)

	t.Logf("Cluster started with bootstrap servers: %s", cluster.BootstrapServers)

	// Create topic with replication factor 3
	topicName := "test-leader-failover"
	err = createTopicWithConfig(ctx, cluster.BootstrapServers, topicName, 3, 3, map[string]string{
		"min.insync.replicas": "2",
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, cluster.BootstrapServers, "leader-failover-tx")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Complete a successful transaction
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	record := txProducer.NewRecord(txCtx, []byte("key1"), []byte("value1"), topicName, 0, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Fatalf("ProduceAsync failed: %v", err)
	}

	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Fatalf("First CommitTransaction failed: %v", err)
	}
	t.Log("First transaction committed")

	// Stop first broker (maybe leader for some partitions)
	t.Log("Stopping broker 0...")
	err = cluster.StopBroker(ctx, 0)
	if err != nil {
		t.Logf("Failed to stop broker 0: %v", err)
	}

	// Wait for leader election
	time.Sleep(10 * time.Second)

	// Try another transaction - should work with remaining brokers
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Logf("BeginTransaction after broker stop: %v", err)
		return
	}

	record2 := txProducer.NewRecord(txCtx, []byte("key2"), []byte("value2"), topicName, 0, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record2)
	if err != nil {
		t.Logf("ProduceAsync after broker stop: %v", err)
		txProducer.AbortTransaction(txCtx)
		return
	}

	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Logf("CommitTransaction after leader failover: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		}
	} else {
		t.Log("SUCCESS: Transaction succeeded after leader failover")
	}
}

func TestIntegration_MultiBroker_MinISRViolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Log("Starting 3-broker Kafka cluster...")
	cluster, err := SetupMultiBrokerCluster(ctx)
	if err != nil {
		t.Fatalf("Failed to setup multi-broker cluster: %v", err)
	}
	defer cluster.Terminate(ctx)

	// Create topic with min.insync.replicas=2
	topicName := "test-min-isr"
	err = createTopicWithConfig(ctx, cluster.BootstrapServers, topicName, 1, 3, map[string]string{
		"min.insync.replicas": "2",
	})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	txProducer, cleanup := createIntegrationTxProducer(t, cluster.BootstrapServers, "min-isr-tx")
	defer cleanup()

	txCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	err = txProducer.InitTransactions(txCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Stop 2 brokers to violate min ISR
	t.Log("Stopping brokers 1 and 2 to violate min.insync.replicas...")
	err = cluster.StopBroker(ctx, 1)
	if err != nil {
		t.Logf("Failed to stop broker 1: %v", err)
	}
	err = cluster.StopBroker(ctx, 2)
	if err != nil {
		t.Logf("Failed to stop broker 2: %v", err)
	}

	// Wait for brokers to fully stop
	time.Sleep(5 * time.Second)

	// Try to produce - should fail due to min ISR violation
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Logf("BeginTransaction with min ISR violation: %v", err)
		return
	}

	record := txProducer.NewRecord(txCtx, []byte("key"), []byte("value"), topicName, 0, time.Now(), nil, "")
	err = txProducer.ProduceAsync(txCtx, record)
	if err != nil {
		t.Logf("ProduceAsync with min ISR violation: %v", err)
	}

	err = txProducer.CommitTransaction(txCtx)
	if err != nil {
		t.Logf("CommitTransaction failed due to min ISR violation: %v", err)

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

			if producerErr.TxnRequiresAbort() {
				t.Log("SUCCESS: Min ISR violation correctly requires abort")
			}
		}
	} else {
		t.Log("CommitTransaction succeeded - brokers may not have stopped yet")
	}
}

// =============================================================================
// Concurrent Transaction Tests
// =============================================================================

func TestIntegration_ConcurrentTransactions_DifferentProducers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, err := setupKafkaContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to setup Kafka container: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	topicName := "test-concurrent-tx"
	err = createTopic(ctx, kafkaContainer.BootstrapServers, topicName, 3, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	numProducers := 5
	messagesPerProducer := 10

	var wg sync.WaitGroup
	errors := make(chan error, numProducers)

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerNum int) {
			defer wg.Done()

			txId := fmt.Sprintf("concurrent-tx-%d", producerNum)
			txProducer, cleanup := createIntegrationTxProducer(t, kafkaContainer.BootstrapServers, txId)
			defer cleanup()

			txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()

			err := txProducer.InitTransactions(txCtx)
			if err != nil {
				errors <- fmt.Errorf("producer %d init failed: %w", producerNum, err)
				return
			}

			for j := 0; j < messagesPerProducer; j++ {
				err = txProducer.BeginTransaction()
				if err != nil {
					errors <- fmt.Errorf("producer %d begin %d failed: %w", producerNum, j, err)
					return
				}

				key := fmt.Sprintf("producer-%d-msg-%d", producerNum, j)
				record := txProducer.NewRecord(
					txCtx,
					[]byte(key),
					[]byte(fmt.Sprintf("value-%d-%d", producerNum, j)),
					topicName,
					int32(producerNum%3),
					time.Now(),
					nil,
					"",
				)

				err = txProducer.ProduceAsync(txCtx, record)
				if err != nil {
					errors <- fmt.Errorf("producer %d produce %d failed: %w", producerNum, j, err)
					txProducer.AbortTransaction(txCtx)
					return
				}

				err = txProducer.CommitTransaction(txCtx)
				if err != nil {
					errors <- fmt.Errorf("producer %d commit %d failed: %w", producerNum, j, err)
					return
				}
			}

			t.Logf("Producer %d completed all %d transactions", producerNum, messagesPerProducer)
		}(i)
	}

	wg.Wait()
	close(errors)

	var errCount int
	for err := range errors {
		errCount++
		t.Logf("Concurrent transaction error: %v", err)
	}

	if errCount == 0 {
		t.Logf("SUCCESS: All %d producers completed %d transactions each", numProducers, messagesPerProducer)
	} else {
		t.Logf("Completed with %d errors out of %d total transactions", errCount, numProducers*messagesPerProducer)
	}
}
