package librd

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	librdKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka/mocks"
	proxyPkg "github.com/gmbyapa/kstream/v2/kafka/mocks/proxy"
	"github.com/google/uuid"
	"github.com/tryfix/metrics/v2"
)

// TestIntegration_InDoubt_CustomProxy uses a custom Kafka protocol-aware proxy
// to achieve a true "in-doubt" transaction scenario where:
// - The commit request reaches the broker and is processed successfully
// - The EndTxn response is dropped by the proxy
// - The producer times out and thinks the commit failed
// - But the transaction was actually committed on the broker
func TestIntegration_InDoubt_CustomProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup Kafka with our custom proxy in front
	// Kafka is configured to advertise the proxy address, so all connections go through it

	testLogger := mocks.NewTestLogger(t)
	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
		mocks.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer cluster.Terminate(context.Background())

	topicName := "test-indoubt-proxy"
	// Create topic through the proxy
	cluster.CreateTopic(t, topicName, 1)

	proxy := cluster.Proxy()

	// Create producer pointing to our proxy
	config := NewProducerConfig()
	config.Id = "indoubt-proxy-test"
	config.BootstrapServers = cluster.BootstrapServers()
	config.Logger = testLogger
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "indoubt-proxy-tx"
	// Experiment: Control retry behavior without transaction.timeout.ms
	// Using only socket and reconnection timeouts
	//config.Librd.SetKey("socket.timeout.ms", 3000)        // 3s - fail fast per request
	//config.Librd.SetKey("reconnect.backoff.ms", 1000)     // 1s initial reconnect delay
	//config.Librd.SetKey("reconnect.backoff.max.ms", 2000) // 2s max reconnect delay
	//config.Librd.SetKey("retries", 5)                     // 2s max reconnect delay
	// Note: retries is INT32_MAX for transactions, cannot override

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(kafka.TransactionalProducer)

	// Initialize transactions (this works normally)
	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initCancel()

	err = txProducer.InitTransactions(initCtx)
	if err != nil {
		t.Fatalf("InitTransactions failed: %v", err)
	}

	// Start a transaction and produce a message
	err = txProducer.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	messageKey := fmt.Sprintf("indoubt-key-%d", time.Now().UnixNano())
	messageValue := fmt.Sprintf("indoubt-value-%d", time.Now().UnixNano())

	produceCtx, produceCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer produceCancel()

	record := txProducer.NewRecord(produceCtx, []byte(messageKey), []byte(messageValue), topicName, -1, time.Now(), nil, "")
	err = txProducer.ProduceAsync(produceCtx, record)
	if err != nil {
		t.Fatalf("ProduceAsync failed: %v", err)
	}
	t.Logf("Message produced: key=%s", messageKey)

	// NOW configure the proxy to DROP EndTxn responses
	// The commit request will reach Kafka, Kafka will commit the transaction,
	// but the response will never reach the producer!
	t.Log("Configuring proxy to DROP EndTxn responses...")
	// Enable closeOnDrop to prevent librdkafka's "Invalid transaction state transition" crash
	// which occurs when the state machine times out but the connection is still alive
	proxy.EnableCloseOnDrop()
	//proxy.EnableVerboseLogging() // Enable to debug error injection
	//proxy.DropFirstNResponsesFor(APIKeyEndTxn, 2)

	proxy.InjectErrorFor(proxyPkg.APIKeyEndTxn, proxyPkg.ErrCoordinatorNotAvailable, 20)
	//proxy.InjectErrorFor(APIKeyProduce, 47, 5)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = txProducer.CommitTransaction(ctx)

	// Check how many EndTxn responses had errors injected
	injectedCount := proxy.GetInjectedCount(proxyPkg.APIKeyEndTxn)
	t.Logf("proxy injected errors into %d EndTxn response(s)", injectedCount)

	proxy.ClearErrorInjection()

	commitSucceeded := false
	if err == nil {
		t.Log("CommitTransaction succeeded (unexpected - response should have been dropped)")
		commitSucceeded = true
	} else {
		t.Logf("CommitTransaction failed as expected: %v", err)
		t.Log("Producer thinks commit failed, but let's check if it actually succeeded...")

		producerErr, ok := err.(Err)
		if ok {
			t.Logf("Error classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
				producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())

			if producerErr.TxnRequiresAbort() {
				t.Log(`transaction aborting...`)
				err = txProducer.AbortTransaction(context.Background())
				if err != nil {
					t.Fatalf("AbortTransaction failed: %v", err)
				}
			}
		}
	}

	// Clear the drop rule so we can consume
	proxy.ClearDropRules()

	// Wait a moment for things to settle
	time.Sleep(2 * time.Second)

	// Verify if the message was actually committed by consuming it
	// IMPORTANT: Connect directly to Kafka, not through the proxy
	t.Log("Checking if message was committed (connecting directly to Kafka)...")

	// First check with read_uncommitted
	// IMPORTANT: Connect directly to Kafka (bypassing the proxy) for verification
	t.Log("Step 1: Checking with read_uncommitted...")
	found, _ := mocks.ConsumeMessageReadUncommitted(t, cluster.BootstrapServers(), topicName, messageKey, 10*time.Second)
	if found {
		t.Log("Message found with read_uncommitted")
	} else {
		t.Log("Message NOT found with read_uncommitted - produce may have failed")
	}

	// Check with read_committed to verify transaction status
	t.Log("Step 2: Checking with read_committed...")
	found, _ = mocks.ConsumeMessageReadCommitted(t, cluster.BootstrapServers(), topicName, messageKey, 10*time.Second)
	if found {
		t.Log("=" + strings.Repeat("=", 60))
		t.Log("SUCCESS: TRUE IN-DOUBT SCENARIO ACHIEVED!")
		t.Log("=" + strings.Repeat("=", 60))
		t.Log("- Producer received error/timeout")
		t.Log("- But commit ACTUALLY SUCCEEDED on the broker!")
		t.Log("- Message is visible with read_committed isolation")
		if !commitSucceeded {
			t.Log("- This is the exact 'in-doubt' transaction scenario!")
		}
		t.Log("=" + strings.Repeat("=", 60))
	} else {
		t.Log("Message NOT found with read_committed")
		if commitSucceeded {
			t.Log("WARNING: Producer thought commit succeeded but message not visible")
		} else {
			t.Log("Commit may have actually failed (not just the response)")
		}
	}

	t.Log("In-doubt proxy test completed")
}

type injectType string

const (
	errInjectTypeResponseDrop  injectType = `InjectTypeResponseDrop`
	errInjectTypeResponseError injectType = `InjectTypeResponseError`
	errInjectTypeRequestDrop   injectType = `InjectTypeRequestDrop`
)

func TestTransactionalProducer_Init(t *testing.T) {
	type fields struct {
		injectType         injectType
		injectApi          proxyPkg.KafkaAPIKey
		injectedError      proxyPkg.KafkaErrorCode
		initCtxTimeout     time.Duration
		injectedErrorCount int
		expectedRetryCount int
	}

	type errorCriteria struct {
		expectedErrorProducerShouldRestart  bool
		expectedErrorProducerShouldAbort    bool
		expectedErrorProducerShouldShutdown bool
	}

	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
		mocks.WithLogger(mocks.NewTestLogger(t)),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer cluster.Terminate(context.Background())

	tests := []struct {
		name                 string
		fields               fields
		errorExpected        bool
		expectedErrorPattern string
		errorCriteria        errorCriteria
	}{
		{
			name: "InitProducerId_NetworkException_ExceedsRetryLimit",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyInitProducerId,
				injectedError:      proxyPkg.ErrNetworkException,
				injectedErrorCount: 999, // Inject near indefinite errors
			},
			errorExpected:        true,
			expectedErrorPattern: `producer error retry count exceeded`,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,
			},
		},
		{
			name: "InitProducerId_NetworkException_ErrCoordinatorNotAvailable_Recover_After_Retry",
			fields: fields{
				injectType:    errInjectTypeResponseError,
				injectApi:     proxyPkg.APIKeyInitProducerId,
				injectedError: proxyPkg.ErrCoordinatorNotAvailable,
				// Inject errors untile librdkafka gives up(this is usually transaction.timeout.ms*2)
				// It should gives up after nearly 10 connective requests.
				injectedErrorCount: 20,
			},
			errorExpected: false,
			errorCriteria: errorCriteria{},
		},
		{
			name: "InitProducerId_NetworkException_ContextTimeout",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyInitProducerId,
				injectedError:      proxyPkg.ErrNetworkException,
				injectedErrorCount: 999,                    // Inject near indefinite errors
				initCtxTimeout:     100 * time.Millisecond, // Simulate a short timeout
			},
			errorExpected:        true,
			expectedErrorPattern: `Context already expired`,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,
			},
		},
		{
			name: "InitProducerId_NetworkException_ProducerFenced_Should_Return_Immidiate_RestartError",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyInitProducerId,
				injectedError:      proxyPkg.ErrProducerFenced,
				injectedErrorCount: 1, // Inject near indefinite errors
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},
		{
			name: "InitProducerId_NetworkException_OutOfOrderSequenceNumber_Should_Return_RestartError",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyInitProducerId,
				injectedError:      proxyPkg.ErrOutOfOrderSequenceNumber,
				injectedErrorCount: 999,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},
		{
			name: "InitProducerId_NetworkException_OutOfOrderSequenceNumber_Should_Return_RestartError",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyInitProducerId,
				injectedError:      proxyPkg.ErrOutOfOrderSequenceNumber,
				injectedErrorCount: 999,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},
		{
			name: "InitProducerId_NetworkException_FencedInstanceID_Should_Return_RestartError",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyInitProducerId,
				injectedError:      proxyPkg.ErrFencedInstanceID,
				injectedErrorCount: 999,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},
		{
			name: "InitProducerId_Request_Drop_Should_Timeout_After_Retry",
			fields: fields{
				injectType:         errInjectTypeRequestDrop,
				injectApi:          proxyPkg.APIKeyInitProducerId,
				injectedErrorCount: 999, // Inject near indefinite errors
			},
			errorExpected:        true,
			expectedErrorPattern: `producer error retry count exceeded`,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,
			},
		},
		{
			name: "InitProducerId_Response_Drop_Should_Timeout_After_Retry",
			fields: fields{
				injectType:         errInjectTypeResponseDrop,
				injectApi:          proxyPkg.APIKeyInitProducerId,
				injectedErrorCount: 999, // Inject near indefinite errors
			},
			errorExpected:        true,
			expectedErrorPattern: `producer error retry count exceeded`,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,
			},
		},
		{
			name: "InitProducerId_Response_Drop_Should_Timeout_After_Retry_XXXXXX",
			fields: fields{
				injectType:         errInjectTypeResponseDrop,
				injectApi:          proxyPkg.APIKeyInitProducerId,
				injectedErrorCount: 999, // Inject near indefinite errors
			},
			errorExpected:        true,
			expectedErrorPattern: `producer error retry count exceeded`,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testLogger := mocks.NewTestLogger(t)
			cluster.Proxy().SetLogger(testLogger)
			defer func() {
				cluster.Proxy().ClearErrorInjection()
				cluster.Proxy().ClearDropRules()
				cluster.Proxy().ClearRequestDropRules()
			}()

			//cluster.Proxy().EnableVerboseLogging()

			// Create producer pointing to our proxy
			config := NewProducerConfig()
			config.Id = "indoubt-proxy-test"
			config.BootstrapServers = cluster.BootstrapServers()
			config.Logger = testLogger
			config.MetricsReporter = metrics.NoopReporter()
			config.Transactional.Enabled = true
			config.Transactional.Id = uuid.New().String()

			config.Librd.SetKey("transaction.timeout.ms", 2000)

			if tt.fields.injectType == errInjectTypeRequestDrop {
				cluster.Proxy().DropRequestsFor(tt.fields.injectApi, tt.fields.injectedErrorCount)
			}

			if tt.fields.injectType == errInjectTypeResponseError {
				cluster.Proxy().InjectErrorFor(tt.fields.injectApi, tt.fields.injectedError, tt.fields.injectedErrorCount)
			}

			if tt.fields.injectType == errInjectTypeResponseDrop {
				cluster.Proxy().DropResponsesFor(tt.fields.injectApi, tt.fields.injectedErrorCount)
			}

			producer, err := NewProducer(config)
			if err != nil {
				t.Fatalf("Failed to create producer: %v", err)
			}
			defer producer.Close()

			txProducer := producer.(kafka.TransactionalProducer)

			ctx := context.Background()
			if tt.fields.initCtxTimeout != time.Duration(0) {
				initCtx, initCancel := context.WithTimeout(context.Background(), tt.fields.initCtxTimeout)
				defer initCancel()
				ctx = initCtx
			}

			err = txProducer.InitTransactions(ctx)

			if tt.fields.expectedRetryCount > 0 {
				var count int
				switch tt.fields.injectType {
				case errInjectTypeRequestDrop:
					count = cluster.Proxy().GetDroppedRequestCount(tt.fields.injectApi)
				case errInjectTypeResponseDrop:
					count = cluster.Proxy().GetDroppedCount(tt.fields.injectApi)
				case errInjectTypeResponseError:
					count = cluster.Proxy().GetInjectedCount(tt.fields.injectApi)
				}

				if count != tt.fields.expectedRetryCount {
					t.Errorf(`Expected retry count to be %d, got %d`, tt.fields.expectedRetryCount, count)
				}
			}

			if err != nil {
				if !tt.errorExpected {
					t.Errorf("InitTransactions failed: %v", err)
				}

				if tt.expectedErrorPattern != `` {
					if !strings.Contains(err.Error(), tt.expectedErrorPattern) {
						t.Errorf(`Expected error message to contain "%s", got "%s"`, tt.expectedErrorPattern, err.Error())
					}
				}

				assertError(
					t,
					err.(kafka.ProducerErr),
					tt.errorCriteria.expectedErrorProducerShouldAbort,
					tt.errorCriteria.expectedErrorProducerShouldRestart,
					tt.errorCriteria.expectedErrorProducerShouldShutdown,
				)
			} else {
				if tt.errorExpected {
					t.Errorf(`Error expected for injectType %s`, tt.fields.injectType)
				}
			}
		})
	}
}

// TestTransactionalProducer_Produce tests error handling during async produce operations.
// The test uses a flush-then-produce-again pattern:
// 1. InitTransactions and BeginTransaction (no injection - should succeed)
// 2. Inject errors for the target API (Produce, AddPartitionsToTxn, etc.)
// 3. ProduceAsync (message 1) - queued, usually returns nil
// 4. Flush() - forces delivery, error occurs internally
// 5. ProduceAsync (message 2) - fails immediately with error state
// 6. Assert the error classification
func TestTransactionalProducer_Produce(t *testing.T) {
	type fields struct {
		injectType         injectType
		injectApi          proxyPkg.KafkaAPIKey
		injectedError      proxyPkg.KafkaErrorCode
		injectedErrorCount int
	}

	type errorCriteria struct {
		expectedErrorProducerShouldRestart  bool
		expectedErrorProducerShouldAbort    bool
		expectedErrorProducerShouldShutdown bool
	}

	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
		mocks.WithLogger(mocks.NewTestLogger(t)),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer cluster.Terminate(context.Background())

	// Create topic for produce tests
	topicName := "test-produce-errors"
	cluster.CreateTopic(t, topicName, 1)

	tests := []struct {
		name                 string
		fields               fields
		errorExpected        bool
		expectedErrorPattern string
		errorCriteria        errorCriteria
	}{
		{
			// Note:: Check comment in producer
			name: "Produce_OutOfOrderSequence_ShouldAbort",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyProduce,
				injectedError:      proxyPkg.ErrOutOfOrderSequenceNumber,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},
		{
			name: "Produce_DuplicateSequenceNumber_ShouldSuccess(Idempotent)",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyProduce,
				injectedError:      proxyPkg.ErrDuplicateSequenceNumber,
				injectedErrorCount: 1,
			},
			errorExpected: false,
			errorCriteria: errorCriteria{},
		},
		{
			// Note:: belongs to an invalid configuration error type as per KIP-1050, but ShouldRestart may be fine for the application layer.
			name: "Produce_InvalidTopicException_ShouldRestart",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyProduce,
				injectedError:      proxyPkg.ErrInvalidTopicException,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},
		{
			// Note:: check comment in producer
			name: "Produce_InvalidProducerEpoch_ShouldRestart",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyProduce,
				injectedError:      proxyPkg.ErrInvalidProducerEpoch,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},
		{
			name: "Produce_ProducerFenced_ShouldRestart",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyProduce,
				injectedError:      proxyPkg.ErrProducerFenced,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},
		{
			// NOT_LEADER_FOR_PARTITION is a retriable error.
			// librdkafka should retry internally and eventually succeed after errors are cleared.
			// We inject only 2 errors to allow recovery before transaction timeout.
			name: "Produce_NotLeaderForPartition_ShouldRetryAndSucceed",
			fields: fields{
				injectType:    errInjectTypeResponseError,
				injectApi:     proxyPkg.APIKeyProduce,
				injectedError: proxyPkg.ErrNotLeaderForPartition,

				injectedErrorCount: 2, // Inject few errors, then succeed
			},
			errorExpected: false, // Should recover after retries
			errorCriteria: errorCriteria{},
		},
		{
			name: "Produce_RequestDrop_ShouldTimeout",
			fields: fields{
				injectType:         errInjectTypeRequestDrop,
				injectApi:          proxyPkg.APIKeyProduce,
				injectedErrorCount: 999, // Drop indefinitely
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},
		{
			// When Produce responses are dropped, the message times out and librdkafka
			// enters an erroneous state with "requires epoch bump" - meaning it wants
			// to restart/reinitialize the producer, not just abort or shutdown.
			name: "Produce_ResponseDrop_ShouldRestart",
			fields: fields{
				injectType:         errInjectTypeResponseDrop,
				injectApi:          proxyPkg.APIKeyProduce,
				injectedErrorCount: 999, // Drop indefinitely
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},
		{
			// INVALID_TXN_STATE indicates the producer is in an inconsistent state.
			// librdkafka treats this as a FATAL error (not just abort-requiring) because
			// the producer's internal state is corrupted and cannot be recovered by abort alone.
			name: "AddPartitionsToTxn_InvalidTxnState_ShouldShutdown",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyAddPartitionsToTxn,
				injectedError:      proxyPkg.ErrInvalidTxnState,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,
			},
		},
		{
			name: "AddPartitionsToTxn_CoordinatorNotAvailable_ShouldRetryAndSucceed",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyAddPartitionsToTxn,
				injectedError:      proxyPkg.ErrCoordinatorNotAvailable,
				injectedErrorCount: 2, // Inject a few errors, then succeed
			},
			errorExpected: false, // Should recover after retries
			errorCriteria: errorCriteria{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testLogger := mocks.NewTestLogger(t)
			cluster.Proxy().SetLogger(testLogger)
			defer func() {
				cluster.Proxy().ClearErrorInjection()
				cluster.Proxy().ClearDropRules()
				cluster.Proxy().ClearRequestDropRules()
			}()

			// Uncomment for debugging
			//cluster.Proxy().EnableVerboseLogging()

			// Delivery report channel for tracking message delivery
			deliveryReportCh := make(chan kafka.DeliveryReport, 10)

			// Create producer with unique transactional ID
			config := NewProducerConfig()
			config.Id = "produce-test"
			config.BootstrapServers = cluster.BootstrapServers()
			config.Logger = testLogger
			config.MetricsReporter = metrics.NoopReporter()
			config.Transactional.Enabled = true
			config.Transactional.Id = uuid.New().String()
			config.OnMessageDelivery = func(report kafka.DeliveryReport) {
				t.Logf("Delivery report: topic=%s partition=%d offset=%d delivered=%v err=%v",
					report.TopicPartition().Topic, report.TopicPartition().Partition,
					report.Offset(), report.Delivered(), report.Error())
				deliveryReportCh <- report
			}
			_ = config.Librd.SetKey("transaction.timeout.ms", 5000)
			_ = config.Librd.SetKey("linger.ms", 0) // Send immediately, no batching delay

			producer, err := NewProducer(config)
			if err != nil {
				t.Fatalf("Failed to create producer: %v", err)
			}
			defer func() {
				_ = producer.Close()
			}()

			txProducer := producer.(kafka.TransactionalProducer)
			ctx := context.Background()

			// Step 1: InitTransactions (no injection - should succeed)
			err = txProducer.InitTransactions(ctx)
			if err != nil {
				t.Fatalf("InitTransactions failed: %v", err)
			}

			// Step 2: BeginTransaction (should succeed)
			err = txProducer.BeginTransaction()
			if err != nil {
				t.Fatalf("BeginTransaction failed: %v", err)
			}

			// Step 3: NOW inject errors for the target API
			switch tt.fields.injectType {
			case errInjectTypeRequestDrop:
				cluster.Proxy().DropRequestsFor(tt.fields.injectApi, tt.fields.injectedErrorCount)
			case errInjectTypeResponseError:
				cluster.Proxy().InjectErrorFor(tt.fields.injectApi, tt.fields.injectedError, tt.fields.injectedErrorCount)
			case errInjectTypeResponseDrop:
				cluster.Proxy().DropResponsesFor(tt.fields.injectApi, tt.fields.injectedErrorCount)
			}

			// Step 4: First ProduceAsync - queues message, usually returns nil
			key1 := []byte(fmt.Sprintf("key1-%d", time.Now().UnixNano()))
			value1 := []byte("test-value-1")
			record1 := txProducer.NewRecord(ctx, key1, value1, topicName, -1, time.Now(), nil, "")

			err = txProducer.ProduceAsync(ctx, record1)
			if err != nil {
				t.Logf("First ProduceAsync returned error (may be expected for some scenarios): %v", err)
			}

			// For success scenarios (retry tests), wait for delivery report to confirm success
			if !tt.errorExpected {
				// Wait for delivery report with timeout
				select {
				case report := <-deliveryReportCh:
					injectedCount := cluster.Proxy().GetInjectedCount(tt.fields.injectApi)
					t.Logf("Errors injected before success: %d", injectedCount)

					if !report.Delivered() {
						t.Errorf("Expected message to be delivered after retry, got error: %v", report.Error())
					} else {
						t.Logf("Message delivered successfully after %d error(s) injected: partition=%d offset=%d",
							injectedCount, report.TopicPartition().Partition, report.Offset())
					}

					// Verify that errors were actually injected (proving retry happened)
					if injectedCount == 0 {
						t.Logf("WARNING: No errors were injected - retry path may not have been exercised")
					}
				case <-time.After(10 * time.Second):
					t.Errorf("Timeout waiting for delivery report - message may not have been delivered")
				}
				return
			}

			// Step 5: Flush - forces delivery, error occurs internally
			// This triggers the actual Kafka Produce request
			txProducer.(*TransactionalProducer).Producer.Flush()

			// Step 6: Second ProduceAsync - should fail immediately if producer is in error state
			key2 := []byte(fmt.Sprintf("key2-%d", time.Now().UnixNano()))
			value2 := []byte("test-value-2")
			record2 := txProducer.NewRecord(ctx, key2, value2, topicName, -1, time.Now(), nil, "")

			err = txProducer.ProduceAsync(ctx, record2)

			// Step 7: Assert expectations
			if err == nil {
				t.Errorf("Expected error but ProduceAsync succeeded")
				return
			}

			if tt.expectedErrorPattern != "" {
				if !strings.Contains(err.Error(), tt.expectedErrorPattern) {
					t.Errorf("Expected error containing %q, got %q", tt.expectedErrorPattern, err.Error())
				}
			}

			producerErr, ok := err.(kafka.ProducerErr)
			if !ok {
				t.Fatalf("Expected kafka.ProducerErr, got %T: %v", err, err)
			}

			assertError(
				t,
				producerErr,
				tt.errorCriteria.expectedErrorProducerShouldAbort,
				tt.errorCriteria.expectedErrorProducerShouldRestart,
				tt.errorCriteria.expectedErrorProducerShouldShutdown,
			)
		})
	}
}

// =============================================================================
// TestTransactionalProducer_SendOffsetsToTransaction tests error handling
// for SendOffsetsToTransaction which uses two Kafka APIs:
// - AddOffsetsToTxn (APIKey 25): Adds consumer group to transaction (top-level error code)
// - TxnOffsetCommit (APIKey 28): Commits offsets (nested partition-level errors)
// =============================================================================

func TestTransactionalProducer_SendOffsetsToTransaction(t *testing.T) {
	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
		mocks.WithLogger(mocks.NewTestLogger(t)),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer cluster.Terminate(context.Background())

	topicName := "test-send-offsets-errors"
	consumerGroupID := "test-send-offsets-group"

	// Create topic
	cluster.CreateTopic(t, topicName, 1)

	type fields struct {
		injectType         injectType
		injectApi          proxyPkg.KafkaAPIKey
		injectedError      proxyPkg.KafkaErrorCode
		injectedErrorCount int
	}

	type errorCriteria struct {
		expectedErrorProducerShouldRestart  bool
		expectedErrorProducerShouldAbort    bool
		expectedErrorProducerShouldShutdown bool
	}

	tests := []struct {
		name                 string
		fields               fields
		errorExpected        bool
		expectedErrorPattern string
		errorCriteria        errorCriteria
	}{
		// =================================================================
		// AddOffsetsToTxn error scenarios (API Key 25)
		// This API has a top-level error code
		// =================================================================
		{
			// NOT_COORDINATOR means the broker is not the coordinator for the group.
			// librdkafka should retry by finding the new coordinator.
			name: "AddOffsetsToTxn_NotCoordinator_ShouldRetryAndSucceed",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyAddOffsetsToTxn,
				injectedError:      proxyPkg.ErrNotCoordinator,
				injectedErrorCount: 2, // Inject few errors, then succeed
			},
			errorExpected: false, // Should recover after retries
			errorCriteria: errorCriteria{},
		},
		{
			// COORDINATOR_NOT_AVAILABLE is retriable - coordinator is temporarily unavailable.
			name: "AddOffsetsToTxn_CoordinatorNotAvailable_ShouldRetryAndSucceed",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyAddOffsetsToTxn,
				injectedError:      proxyPkg.ErrCoordinatorNotAvailable,
				injectedErrorCount: 2,
			},
			errorExpected: false,
			errorCriteria: errorCriteria{},
		},
		{
			// INVALID_TXN_STATE indicates the producer is in an inconsistent state.
			// librdkafka treats this as a FATAL error requiring shutdown.
			name: "AddOffsetsToTxn_InvalidTxnState_ShouldShutdown",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyAddOffsetsToTxn,
				injectedError:      proxyPkg.ErrInvalidTxnState,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,
			},
		},
		{
			// TRANSACTIONAL_ID_AUTHORIZATION_FAILED is a fatal error.
			name: "AddOffsetsToTxn_TransactionalIdAuthorizationFailed_ShouldShutdown",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyAddOffsetsToTxn,
				injectedError:      proxyPkg.ErrTransactionalIDAuthorizationFail,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,
			},
		},
		{
			// PRODUCER_FENCED means another producer with the same transactional.id has started.
			// Requires restart with a new producer instance.
			name: "AddOffsetsToTxn_ProducerFenced_ShouldRestart",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyAddOffsetsToTxn,
				injectedError:      proxyPkg.ErrProducerFenced,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,
			},
		},

		// =================================================================
		// TxnOffsetCommit error scenarios (API Key 28)
		// This API has nested partition-level error codes
		// =================================================================
		{
			// GROUP_AUTHORIZATION_FAILED is a fatal error - not authorized for the consumer group.
			name: "TxnOffsetCommit_GroupAuthorizationFailed_ShouldShutdown",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyTxnOffsetCommit,
				injectedError:      proxyPkg.ErrGroupAuthorizationFailed,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldAbort: true,
			},
		},
		{
			// UNKNOWN_TOPIC_OR_PARTITION is a retriable error.
			name: "TxnOffsetCommit_UnknownTopicOrPartition_ShouldAbort_After_Retries",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyTxnOffsetCommit,
				injectedError:      proxyPkg.ErrUnknownTopicOrPartition,
				injectedErrorCount: 20,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldAbort: true,
			},
		},
		{
			// UNKNOWN_TOPIC_OR_PARTITION is a retriable error.
			name: "TxnOffsetCommit_UnknownTopicOrPartition_ShouldRetryAndSucceed",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyTxnOffsetCommit,
				injectedError:      proxyPkg.ErrUnknownTopicOrPartition,
				injectedErrorCount: 3,
			},
			errorExpected: false,
			errorCriteria: errorCriteria{},
		},
		{
			// COORDINATOR_LOAD_IN_PROGRESS is retriable - coordinator is initializing.
			name: "TxnOffsetCommit_CoordinatorLoadInProgress_ShouldRetryAndSucceed",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyTxnOffsetCommit,
				injectedError:      proxyPkg.ErrCoordinatorLoadInProgress,
				injectedErrorCount: 2,
			},
			errorExpected: false,
			errorCriteria: errorCriteria{},
		},

		// =================================================================
		// Response drop scenarios
		// =================================================================
		{
			// When AddOffsetsToTxn response is dropped, the request times out.
			name: "AddOffsetsToTxn_ResponseDrop_ShouldTimeout",
			fields: fields{
				injectType:         errInjectTypeResponseDrop,
				injectApi:          proxyPkg.APIKeyAddOffsetsToTxn,
				injectedErrorCount: 999,
			},
			errorExpected:        true,
			expectedErrorPattern: `producer error retry count exceeded`,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testLogger := mocks.NewTestLogger(t)
			cluster.Proxy().SetLogger(testLogger)
			defer func() {
				cluster.Proxy().ClearErrorInjection()
				cluster.Proxy().ClearDropRules()
				cluster.Proxy().ClearRequestDropRules()
			}()

			// Enable verbose logging for debugging
			//cluster.Proxy().EnableVerboseLogging()

			// Create a consumer to get group metadata
			consumer, err := librdKafka.NewConsumer(&librdKafka.ConfigMap{
				"bootstrap.servers": strings.Join(cluster.BootstrapServers(), ","),
				"group.id":          consumerGroupID + "-" + uuid.New().String(), // Unique group per test
				"auto.offset.reset": "earliest",
			})
			if err != nil {
				t.Fatalf("Failed to create consumer: %v", err)
			}
			defer consumer.Close()

			// Subscribe and poll to join group
			err = consumer.Subscribe(topicName, nil)
			if err != nil {
				t.Fatalf("Failed to subscribe: %v", err)
			}
			consumer.Poll(5000) // Trigger group join

			cgmd, err := consumer.GetConsumerGroupMetadata()
			if err != nil {
				t.Fatalf("Failed to get consumer group metadata: %v", err)
			}

			// Create producer with unique transactional ID
			config := NewProducerConfig()
			config.Id = "send-offsets-test"
			config.BootstrapServers = cluster.BootstrapServers()
			config.Logger = testLogger
			config.MetricsReporter = metrics.NoopReporter()
			config.Transactional.Enabled = true
			config.Transactional.Id = uuid.New().String()
			_ = config.Librd.SetKey("transaction.timeout.ms", 5000)

			producer, err := NewProducer(config)
			if err != nil {
				t.Fatalf("Failed to create producer: %v", err)
			}
			defer func() {
				_ = producer.Close()
			}()

			txProducer := producer.(kafka.TransactionalProducer)
			ctx := context.Background()

			// Step 1: InitTransactions (should succeed)
			err = txProducer.InitTransactions(ctx)
			if err != nil {
				t.Fatalf("InitTransactions failed: %v", err)
			}

			// Step 2: BeginTransaction (should succeed)
			err = txProducer.BeginTransaction()
			if err != nil {
				t.Fatalf("BeginTransaction failed: %v", err)
			}

			// Step 3: NOW inject errors for the target API
			switch tt.fields.injectType {
			case errInjectTypeRequestDrop:
				cluster.Proxy().DropRequestsFor(tt.fields.injectApi, tt.fields.injectedErrorCount)
			case errInjectTypeResponseError:
				cluster.Proxy().InjectErrorFor(tt.fields.injectApi, tt.fields.injectedError, tt.fields.injectedErrorCount)
			case errInjectTypeResponseDrop:
				cluster.Proxy().DropResponsesFor(tt.fields.injectApi, tt.fields.injectedErrorCount)
			}

			// Step 4: SendOffsetsToTransaction
			offsets := []kafka.ConsumerOffset{
				{Topic: topicName, Partition: 0, Offset: 10, Meta: ""},
			}
			groupMeta := &kafka.GroupMeta{Meta: cgmd}

			err = txProducer.SendOffsetsToTransaction(ctx, offsets, groupMeta)

			// Step 5: Assert expectations
			if tt.errorExpected {
				if err == nil {
					t.Errorf("Expected error but SendOffsetsToTransaction succeeded")
					return
				}

				if tt.expectedErrorPattern != "" {
					if !strings.Contains(err.Error(), tt.expectedErrorPattern) {
						t.Errorf("Expected error containing %q, got %q", tt.expectedErrorPattern, err.Error())
					}
				}

				producerErr, ok := err.(kafka.ProducerErr)
				if !ok {
					t.Fatalf("Expected kafka.ProducerErr, got %T: %v", err, err)
				}

				assertError(
					t,
					producerErr,
					tt.errorCriteria.expectedErrorProducerShouldAbort,
					tt.errorCriteria.expectedErrorProducerShouldRestart,
					tt.errorCriteria.expectedErrorProducerShouldShutdown,
				)
			} else {
				if err != nil {
					t.Errorf("Expected success but got error: %v", err)
				} else {
					t.Logf("SendOffsetsToTransaction succeeded after %d error(s) injected",
						cluster.Proxy().GetInjectedCount(tt.fields.injectApi))
				}
			}
		})
	}
}

//func TestTransactionalProducer_CommitBacup_DELETEME(t *testing.T) {
//	cluster, err := mocks.SetupKafkaCluster(t,
//		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
//		mocks.WithLogger(mocks.NewTestLogger(t)),
//	)
//	if err != nil {
//		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
//	}
//	defer cluster.Terminate(context.Background())
//
//	topicName := "test-commit-errors"
//
//	// Create topic
//	cluster.CreateTopic(t, topicName, 1)
//
//	type fields struct {
//		injectType         injectType
//		injectApi          proxyPkg.KafkaAPIKey
//		injectedError      proxyPkg.KafkaErrorCode
//		injectedErrorCount int
//	}
//
//	type errorCriteria struct {
//		expectedErrorProducerShouldRestart  bool
//		expectedErrorProducerShouldAbort    bool
//		expectedErrorProducerShouldShutdown bool
//	}
//
//	tests := []struct {
//		name                 string
//		fields               fields
//		errorExpected        bool
//		expectedErrorPattern string
//		errorCriteria        errorCriteria
//	}{
//		// =================================================================
//		// EndTxn retriable error scenarios (API Key 26)
//		// These errors should be retried internally by librdkafka
//		// =================================================================
//		{
//			// COORDINATOR_NOT_AVAILABLE is retriable - coordinator is temporarily unavailable.
//			// librdkafka should retry internally and eventually succeed.
//			name: "EndTxn_CoordinatorNotAvailable_ShouldRetryAndSucceed",
//			fields: fields{
//				injectType:         errInjectTypeResponseError,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedError:      proxyPkg.ErrCoordinatorNotAvailable,
//				injectedErrorCount: 2, // Inject few errors, then succeed
//			},
//			errorExpected: false, // Should recover after retries
//			errorCriteria: errorCriteria{},
//		},
//		{
//			// NOT_COORDINATOR means the broker is not the transaction coordinator.
//			// librdkafka should retry by finding the new coordinator.
//			name: "EndTxn_NotCoordinator_ShouldRetryAndSucceed",
//			fields: fields{
//				injectType:         errInjectTypeResponseError,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedError:      proxyPkg.ErrNotCoordinator,
//				injectedErrorCount: 2,
//			},
//			errorExpected: false,
//			errorCriteria: errorCriteria{},
//		},
//		{
//			// CONCURRENT_TRANSACTIONS means another transaction is in progress.
//			// librdkafka should retry after a backoff.
//			name: "EndTxn_ConcurrentTransactions_ShouldRetryAndSucceed",
//			fields: fields{
//				injectType:         errInjectTypeResponseError,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedError:      proxyPkg.ErrConcurrentTransactions,
//				injectedErrorCount: 2,
//			},
//			errorExpected: false,
//			errorCriteria: errorCriteria{},
//		},
//
//		// =================================================================
//		// EndTxn fatal error scenarios requiring shutdown (API Key 26)
//		// These errors indicate unrecoverable state corruption
//		// =================================================================
//		{
//			// INVALID_TXN_STATE indicates the producer is in an inconsistent state.
//			// librdkafka treats this as a FATAL error requiring shutdown.
//			name: "EndTxn_InvalidTxnState_ShouldShutdown",
//			fields: fields{
//				injectType:         errInjectTypeResponseError,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedError:      proxyPkg.ErrInvalidTxnState,
//				injectedErrorCount: 1,
//			},
//			errorExpected: true,
//			errorCriteria: errorCriteria{
//				expectedErrorProducerShouldShutdown: true,
//			},
//		},
//		{
//			// TRANSACTIONAL_ID_AUTHORIZATION_FAILED is a fatal authorization error.
//			name: "EndTxn_TransactionalIdAuthorizationFailed_ShouldShutdown",
//			fields: fields{
//				injectType:         errInjectTypeResponseError,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedError:      proxyPkg.ErrTransactionalIDAuthorizationFail,
//				injectedErrorCount: 1,
//			},
//			errorExpected: true,
//			errorCriteria: errorCriteria{
//				expectedErrorProducerShouldShutdown: true,
//			},
//		},
//		{
//			// UNKNOWN_PRODUCER_ID indicates the producer ID is not known by the broker.
//			// This typically means the transaction has timed out and requires restart.
//			name: "EndTxn_UnknownProducerId_ShouldRestart",
//			fields: fields{
//				injectType:         errInjectTypeResponseError,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedError:      proxyPkg.ErrUnknownProducerID,
//				injectedErrorCount: 1,
//			},
//			errorExpected: true,
//			errorCriteria: errorCriteria{
//				expectedErrorProducerShouldRestart: true,
//			},
//		},
//
//		// =================================================================
//		// EndTxn fencing error scenarios requiring restart (API Key 26)
//		// These errors indicate the producer has been fenced by another instance
//		// =================================================================
//		{
//			// PRODUCER_FENCED means another producer with the same transactional.id has started.
//			// Requires restart with a new producer instance.
//			name: "EndTxn_ProducerFenced_ShouldRestart",
//			fields: fields{
//				injectType:         errInjectTypeResponseError,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedError:      proxyPkg.ErrProducerFenced,
//				injectedErrorCount: 1,
//			},
//			errorExpected: true,
//			errorCriteria: errorCriteria{
//				expectedErrorProducerShouldRestart: true,
//			},
//		},
//		{
//			// INVALID_PRODUCER_EPOCH indicates the producer epoch is stale.
//			// Requires restart with a new producer instance.
//			name: "EndTxn_InvalidProducerEpoch_ShouldRestart",
//			fields: fields{
//				injectType:         errInjectTypeResponseError,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedError:      proxyPkg.ErrInvalidProducerEpoch,
//				injectedErrorCount: 1,
//			},
//			errorExpected: true,
//			errorCriteria: errorCriteria{
//				expectedErrorProducerShouldRestart: true,
//			},
//		},
//
//		// =================================================================
//		// EndTxn timeout and drop scenarios (API Key 26)
//		// =================================================================
//		{
//			// When EndTxn response is dropped indefinitely, the request times out.
//			// This is the "in-doubt" transaction scenario where we don't know if commit succeeded.
//			name: "EndTxn_ResponseDrop_ShouldTimeout",
//			fields: fields{
//				injectType:         errInjectTypeResponseDrop,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedErrorCount: 999,
//			},
//			errorExpected:        true,
//			expectedErrorPattern: `producer error retry count exceeded`,
//			errorCriteria: errorCriteria{
//				expectedErrorProducerShouldShutdown: true,
//			},
//		},
//		{
//			// When EndTxn request is dropped indefinitely, the commit never reaches the broker.
//			name: "EndTxn_RequestDrop_ShouldTimeout",
//			fields: fields{
//				injectType:         errInjectTypeRequestDrop,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedErrorCount: 999,
//			},
//			errorExpected:        true,
//			expectedErrorPattern: `producer error retry count exceeded`,
//			errorCriteria: errorCriteria{
//				expectedErrorProducerShouldShutdown: true,
//			},
//		},
//
//		// =================================================================
//		// EndTxn abort-requiring error scenarios (API Key 26)
//		// =================================================================
//		{
//			// COORDINATOR_LOAD_IN_PROGRESS with excessive retries should abort.
//			name: "EndTxn_CoordinatorLoadInProgress_ShouldAbort_After_Retries",
//			fields: fields{
//				injectType:         errInjectTypeResponseError,
//				injectApi:          proxyPkg.APIKeyEndTxn,
//				injectedError:      proxyPkg.ErrCoordinatorLoadInProgress,
//				injectedErrorCount: 20, // Inject many errors to exhaust retries
//			},
//			errorExpected: true,
//			errorCriteria: errorCriteria{
//				expectedErrorProducerShouldAbort: true,
//			},
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			testLogger := mocks.NewTestLogger(t)
//			cluster.Proxy().SetLogger(testLogger)
//			defer func() {
//				cluster.Proxy().ClearErrorInjection()
//				cluster.Proxy().ClearDropRules()
//				cluster.Proxy().ClearRequestDropRules()
//			}()
//
//			// Enable verbose logging for debugging
//			//cluster.Proxy().EnableVerboseLogging()
//
//			// Create producer with unique transactional ID
//			config := NewProducerConfig()
//			config.Id = "commit-test"
//			config.BootstrapServers = cluster.BootstrapServers()
//			config.Logger = testLogger
//			config.MetricsReporter = metrics.NoopReporter()
//			config.Transactional.Enabled = true
//			config.Transactional.Id = uuid.New().String()
//			_ = config.Librd.SetKey("transaction.timeout.ms", 5000)
//			_ = config.Librd.SetKey("linger.ms", 0) // Send immediately
//
//			producer, err := NewProducer(config)
//			if err != nil {
//				t.Fatalf("Failed to create producer: %v", err)
//			}
//			defer func() {
//				_ = producer.Close()
//			}()
//
//			txProducer := producer.(kafka.TransactionalProducer)
//			ctx := context.Background()
//
//			// Step 1: InitTransactions (should succeed)
//			err = txProducer.InitTransactions(ctx)
//			if err != nil {
//				t.Fatalf("InitTransactions failed: %v", err)
//			}
//
//			// Step 2: BeginTransaction (should succeed)
//			err = txProducer.BeginTransaction()
//			if err != nil {
//				t.Fatalf("BeginTransaction failed: %v", err)
//			}
//
//			// Step 3: Produce a message so there's something to commit
//			key := []byte(fmt.Sprintf("key-%d", time.Now().UnixNano()))
//			value := []byte("test-value")
//			record := txProducer.NewRecord(ctx, key, value, topicName, -1, time.Now(), nil, "")
//
//			err = txProducer.ProduceAsync(ctx, record)
//			if err != nil {
//				t.Fatalf("ProduceAsync failed: %v", err)
//			}
//
//			// Flush to ensure message is sent before commit
//			txProducer.(*TransactionalProducer).Producer.Flush()
//
//			// Step 4: NOW inject errors for the target API (EndTxn)
//			switch tt.fields.injectType {
//			case errInjectTypeRequestDrop:
//				cluster.Proxy().DropRequestsFor(tt.fields.injectApi, tt.fields.injectedErrorCount)
//			case errInjectTypeResponseError:
//				cluster.Proxy().InjectErrorFor(tt.fields.injectApi, tt.fields.injectedError, tt.fields.injectedErrorCount)
//			case errInjectTypeResponseDrop:
//				cluster.Proxy().DropResponsesFor(tt.fields.injectApi, tt.fields.injectedErrorCount)
//			}
//
//			// Step 5: CommitTransaction - this is what we're testing
//			err = txProducer.CommitTransaction(ctx)
//
//			// Step 6: Assert expectations
//			if tt.errorExpected {
//				if err == nil {
//					t.Errorf("Expected error but CommitTransaction succeeded")
//					return
//				}
//
//				if tt.expectedErrorPattern != "" {
//					if !strings.Contains(err.Error(), tt.expectedErrorPattern) {
//						t.Errorf("Expected error containing %q, got %q", tt.expectedErrorPattern, err.Error())
//					}
//				}
//
//				producerErr, ok := err.(kafka.ProducerErr)
//				if !ok {
//					t.Fatalf("Expected kafka.ProducerErr, got %T: %v", err, err)
//				}
//
//				assertError(
//					t,
//					producerErr,
//					tt.errorCriteria.expectedErrorProducerShouldAbort,
//					tt.errorCriteria.expectedErrorProducerShouldRestart,
//					tt.errorCriteria.expectedErrorProducerShouldShutdown,
//				)
//			} else {
//				if err != nil {
//					t.Errorf("Expected success but got error: %v", err)
//				} else {
//					t.Logf("CommitTransaction succeeded after %d error(s) injected",
//						cluster.Proxy().GetInjectedCount(tt.fields.injectApi))
//				}
//			}
//		})
//	}
//}

func TestTransactionalProducer_Commit(t *testing.T) {
	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
		mocks.WithLogger(mocks.NewTestLogger(t)),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer cluster.Terminate(context.Background())

	topicName := "test-commit-errors"

	// Create topic
	cluster.CreateTopic(t, topicName, 1)

	type fields struct {
		injectType         injectType
		injectApi          proxyPkg.KafkaAPIKey
		injectedError      proxyPkg.KafkaErrorCode
		injectedErrorCount int
	}

	type errorCriteria struct {
		expectedErrorProducerShouldRestart  bool
		expectedErrorProducerShouldAbort    bool
		expectedErrorProducerShouldShutdown bool
		// Message verification fields
		expectMessageVisibleUncommitted bool // Message should be visible to read_uncommitted consumer
		expectMessageVisibleCommitted   bool // Message should be visible to read_committed consumer
		isInDoubtScenario               bool // For timeout scenarios where commit outcome is uncertain
	}

	tests := []struct {
		name                 string
		fields               fields
		errorExpected        bool
		expectedErrorPattern string
		errorCriteria        errorCriteria
	}{
		// =================================================================
		// EndTxn retriable error scenarios (API Key 26)
		// These errors should be retried internally by librdkafka
		// =================================================================
		{
			// COORDINATOR_NOT_AVAILABLE is retriable - coordinator is temporarily unavailable.
			// librdkafka should retry internally and eventually succeed.
			name: "EndTxn_CoordinatorNotAvailable_ShouldRetryAndSucceed",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyEndTxn,
				injectedError:      proxyPkg.ErrCoordinatorNotAvailable,
				injectedErrorCount: 2, // Inject few errors, then succeed
			},
			errorExpected: false, // Should recover after retries
			errorCriteria: errorCriteria{
				expectMessageVisibleUncommitted: true,
				expectMessageVisibleCommitted:   true, // Commit succeeds after retry
			},
		},
		{
			// NOT_COORDINATOR means the broker is not the transaction coordinator.
			// librdkafka should retry by finding the new coordinator.
			name: "EndTxn_NotCoordinator_ShouldRetryAndSucceed",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyEndTxn,
				injectedError:      proxyPkg.ErrNotCoordinator,
				injectedErrorCount: 2,
			},
			errorExpected: false,
			errorCriteria: errorCriteria{
				expectMessageVisibleUncommitted: true,
				expectMessageVisibleCommitted:   true, // Commit succeeds after retry
			},
		},
		{
			// CONCURRENT_TRANSACTIONS means another transaction is in progress.
			// librdkafka should retry after a backoff.
			name: "EndTxn_ConcurrentTransactions_ShouldRetryAndSucceed",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyEndTxn,
				injectedError:      proxyPkg.ErrConcurrentTransactions,
				injectedErrorCount: 2,
			},
			errorExpected: false,
			errorCriteria: errorCriteria{
				expectMessageVisibleUncommitted: true,
				expectMessageVisibleCommitted:   true, // Commit succeeds after retry
			},
		},

		// =================================================================
		// EndTxn fatal error scenarios requiring shutdown (API Key 26)
		// These errors indicate unrecoverable state corruption
		// =================================================================
		{
			// INVALID_TXN_STATE indicates the producer is in an inconsistent state.
			// librdkafka treats this as a FATAL error requiring shutdown.
			name: "EndTxn_InvalidTxnState_ShouldShutdown",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyEndTxn,
				injectedError:      proxyPkg.ErrInvalidTxnState,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,

				expectMessageVisibleUncommitted: true, // Message was produced
				expectMessageVisibleCommitted:   true, // Commit is successful in the broker
			},
		},
		{
			// TRANSACTIONAL_ID_AUTHORIZATION_FAILED is a fatal authorization error.
			name: "EndTxn_TransactionalIdAuthorizationFailed_ShouldShutdown",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyEndTxn,
				injectedError:      proxyPkg.ErrTransactionalIDAuthorizationFail,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldShutdown: true,

				expectMessageVisibleUncommitted: true, // Message was produced
				expectMessageVisibleCommitted:   true, // Commit is successful in the broker
			},
		},
		{
			// UNKNOWN_PRODUCER_ID indicates the producer ID is not known by the broker.
			// This typically means the transaction has timed out and requires restart.
			name: "EndTxn_UnknownProducerId_ShouldRestart",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyEndTxn,
				injectedError:      proxyPkg.ErrUnknownProducerID,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,

				expectMessageVisibleUncommitted: true, // Message was produced
				expectMessageVisibleCommitted:   true, // Commit is successful in the broker
			},
		},

		// =================================================================
		// EndTxn fencing error scenarios requiring restart (API Key 26)
		// These errors indicate the producer has been fenced by another instance
		// =================================================================
		{
			// PRODUCER_FENCED means another producer with the same transactional.id has started.
			// Requires restart with a new producer instance.
			name: "EndTxn_ProducerFenced_ShouldRestart",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyEndTxn,
				injectedError:      proxyPkg.ErrProducerFenced,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,

				expectMessageVisibleUncommitted: true, // Message was produced
				expectMessageVisibleCommitted:   true, // Commit is successful in the broker
			},
		},
		{
			// INVALID_PRODUCER_EPOCH indicates the producer epoch is stale.
			// Requires restart with a new producer instance.
			name: "EndTxn_InvalidProducerEpoch_ShouldRestart",
			fields: fields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyEndTxn,
				injectedError:      proxyPkg.ErrInvalidProducerEpoch,
				injectedErrorCount: 1,
			},
			errorExpected: true,
			errorCriteria: errorCriteria{
				expectedErrorProducerShouldRestart: true,

				expectMessageVisibleUncommitted: true, // Message was produced
				expectMessageVisibleCommitted:   true, // Commit is successful in the broker
			},
		},

		// =================================================================
		// NOTE: The following scenarios CANNOT be tested with librdkafka:
		// =================================================================
		// - EndTxn_ResponseDrop_ShouldTimeout (in-doubt scenario)
		// - EndTxn_RequestDrop_ShouldTimeout
		// - EndTxn_CoordinatorLoadInProgress_ShouldAbort_After_Retries
		//
		// Reason: librdkafka retries EndTxn operations indefinitely until
		// transaction.timeout.ms expires. This is by design - abandoning a
		// commit mid-way would leave the transaction in an undefined state.
		// These tests would either run indefinitely or behave unpredictably.
		// =================================================================
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testLogger := mocks.NewTestLogger(t)
			cluster.Proxy().SetLogger(testLogger)
			defer func() {
				cluster.Proxy().ClearErrorInjection()
				cluster.Proxy().ClearDropRules()
				cluster.Proxy().ClearRequestDropRules()
			}()

			// Enable verbose logging for debugging
			//cluster.Proxy().EnableVerboseLogging()

			// Create producer with unique transactional ID
			config := NewProducerConfig()
			config.Id = "commit-test"
			config.BootstrapServers = cluster.BootstrapServers()
			config.Logger = testLogger
			config.MetricsReporter = metrics.NoopReporter()
			config.Transactional.Enabled = true
			config.Transactional.Id = uuid.New().String()
			_ = config.Librd.SetKey("transaction.timeout.ms", 5000)
			_ = config.Librd.SetKey("linger.ms", 0) // Send immediately

			producer, err := NewProducer(config)
			if err != nil {
				t.Fatalf("Failed to create producer: %v", err)
			}
			defer func() {
				_ = producer.Close()
			}()

			txProducer := producer.(kafka.TransactionalProducer)
			ctx := context.Background()

			// Step 1: InitTransactions (should succeed)
			err = txProducer.InitTransactions(ctx)
			if err != nil {
				t.Fatalf("InitTransactions failed: %v", err)
			}

			// Step 2: BeginTransaction (should succeed)
			err = txProducer.BeginTransaction()
			if err != nil {
				t.Fatalf("BeginTransaction failed: %v", err)
			}

			// Step 3: Produce a message so there's something to commit
			key := []byte(fmt.Sprintf("key-%d", time.Now().UnixNano()))
			value := []byte("test-value")
			record := txProducer.NewRecord(ctx, key, value, topicName, -1, time.Now(), nil, "")

			err = txProducer.ProduceAsync(ctx, record)
			if err != nil {
				t.Fatalf("ProduceAsync failed: %v", err)
			}

			// Flush to ensure message is sent before commit
			txProducer.(*TransactionalProducer).Producer.Flush()

			// Step 4: NOW inject errors for the target API (EndTxn)
			switch tt.fields.injectType {
			case errInjectTypeRequestDrop:
				cluster.Proxy().DropRequestsFor(tt.fields.injectApi, tt.fields.injectedErrorCount)
			case errInjectTypeResponseError:
				cluster.Proxy().InjectErrorFor(tt.fields.injectApi, tt.fields.injectedError, tt.fields.injectedErrorCount)
			case errInjectTypeResponseDrop:
				cluster.Proxy().DropResponsesFor(tt.fields.injectApi, tt.fields.injectedErrorCount)
			}

			// Step 5: CommitTransaction - this is what we're testing
			err = txProducer.CommitTransaction(ctx)

			// Step 6: Assert expectations
			if tt.errorExpected {
				if err == nil {
					t.Errorf("Expected error but CommitTransaction succeeded")
					return
				}

				if tt.expectedErrorPattern != "" {
					if !strings.Contains(err.Error(), tt.expectedErrorPattern) {
						t.Errorf("Expected error containing %q, got %q", tt.expectedErrorPattern, err.Error())
					}
				}

				producerErr, ok := err.(kafka.ProducerErr)
				if !ok {
					t.Fatalf("Expected kafka.ProducerErr, got %T: %v", err, err)
				}

				assertError(
					t,
					producerErr,
					tt.errorCriteria.expectedErrorProducerShouldAbort,
					tt.errorCriteria.expectedErrorProducerShouldRestart,
					tt.errorCriteria.expectedErrorProducerShouldShutdown,
				)
			} else {
				if err != nil {
					t.Errorf("Expected success but got error: %v", err)
				} else {
					t.Logf("CommitTransaction succeeded after %d error(s) injected",
						cluster.Proxy().GetInjectedCount(tt.fields.injectApi))
				}
			}

			// Step 7: Message verification using consumer isolation levels
			// Clear proxy rules before verification to ensure clean consumer connections
			cluster.Proxy().ClearErrorInjection()
			cluster.Proxy().ClearDropRules()
			cluster.Proxy().ClearRequestDropRules()

			// Give broker a moment to process any pending state changes
			time.Sleep(500 * time.Millisecond)

			keyStr := string(key)
			verifyTimeout := 5 * time.Second

			// Check message visibility with read_uncommitted
			if tt.errorCriteria.expectMessageVisibleUncommitted {
				foundUncommitted, _ := mocks.ConsumeMessageReadUncommitted(t, cluster.BootstrapServers(), topicName, keyStr, verifyTimeout)
				if !foundUncommitted {
					t.Errorf("Expected message to be visible with read_uncommitted, but it was NOT found")
				} else {
					t.Logf("Message correctly visible with read_uncommitted")
				}
			}

			// Check message visibility with read_committed
			if tt.errorCriteria.isInDoubtScenario {
				// For in-doubt scenarios, log the result but don't assert
				// because we genuinely don't know the outcome
				foundCommitted, _ := mocks.ConsumeMessageReadCommitted(t, cluster.BootstrapServers(), topicName, keyStr, verifyTimeout)
				if foundCommitted {
					t.Logf("IN-DOUBT: Message IS visible with read_committed - commit may have succeeded on broker")
				} else {
					t.Logf("IN-DOUBT: Message NOT visible with read_committed - commit may have failed")
				}
			} else if tt.errorCriteria.expectMessageVisibleCommitted {
				foundCommitted, _ := mocks.ConsumeMessageReadCommitted(t, cluster.BootstrapServers(), topicName, keyStr, verifyTimeout)
				if !foundCommitted {
					t.Errorf("Expected message to be visible with read_committed (commit should have succeeded), but it was NOT found")
				} else {
					t.Logf("Message correctly visible with read_committed (transaction committed)")
				}
			} else if tt.errorCriteria.expectMessageVisibleUncommitted && !tt.errorCriteria.expectMessageVisibleCommitted {
				// Message should be uncommitted - visible to uncommitted but NOT to committed
				foundCommitted, _ := mocks.ConsumeMessageReadCommitted(t, cluster.BootstrapServers(), topicName, keyStr, verifyTimeout)
				if foundCommitted {
					t.Errorf("Expected message to NOT be visible with read_committed (commit should have failed), but it WAS found")
				} else {
					t.Logf("Message correctly NOT visible with read_committed (transaction not committed)")
				}
			}
		})
	}
}

// =============================================================================
// TestTransactionalProducer_Fencing tests producer fencing scenarios using
// actual producers (not proxy-based error injection).
//
// Producer fencing occurs when a new producer starts with the same transactional.id
// as an existing producer. The broker assigns a higher epoch to the new producer,
// invalidating (fencing) the old one.
// =============================================================================

type fencePoint string

const (
	fencePointAfterCommit fencePoint = "AfterCommit"
)

// =============================================================================
// NOTE: FenceDuringProduce and FenceDuringCommit scenarios CANNOT be tested:
// =============================================================================
// Kafka blocks any new producer from acquiring a PID while there's an active
// transaction with the same transactional.id. The new producer gets
// CONCURRENT_TRANSACTIONS error and retries indefinitely until the existing
// transaction completes (commit/abort) or times out.
//
// Therefore, true "mid-transaction fencing" is not possible - fencing only
// occurs AFTER the existing transaction ends.
// =============================================================================

func TestTransactionalProducer_Fencing(t *testing.T) {
	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithLogger(mocks.NewTestLogger(t)),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka cluster: %v", err)
	}
	defer cluster.Terminate(context.Background())

	topicName := "test-fencing"
	cluster.CreateTopic(t, topicName, 1)

	type expectedBehavior struct {
		p1ErrorExpected             bool
		p1ErrorRequiresRestart      bool
		p1MessageVisibleUncommitted bool
		p1MessageVisibleCommitted   bool
	}

	tests := []struct {
		name             string
		fencePoint       fencePoint
		expectedBehavior expectedBehavior
	}{
		{
			// Producer P1 completes a full transaction (produce + commit).
			// Producer P2 starts with same transactional.id and fences P1.
			// P1's next BeginTransaction should fail.
			name:       "FenceAfterCommit",
			fencePoint: fencePointAfterCommit,
			expectedBehavior: expectedBehavior{
				p1ErrorExpected:             true,
				p1ErrorRequiresRestart:      true,
				p1MessageVisibleUncommitted: true, // First transaction succeeded
				p1MessageVisibleCommitted:   true, // First transaction was committed
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testLogger := mocks.NewTestLogger(t)

			// Use a unique transactional.id for each test run to avoid interference
			sharedTxId := fmt.Sprintf("shared-tx-%s-%d", tt.name, time.Now().UnixNano())

			// ============================================================
			// Step 1: Create Producer P1 with the shared transactional.id
			// ============================================================
			configP1 := NewProducerConfig()
			configP1.Id = "producer-p1"
			configP1.BootstrapServers = cluster.BootstrapServers()
			configP1.Logger = testLogger
			configP1.MetricsReporter = metrics.NoopReporter()
			configP1.Transactional.Enabled = true
			configP1.Transactional.Id = sharedTxId
			_ = configP1.Librd.SetKey("transaction.timeout.ms", 10000)
			_ = configP1.Librd.SetKey("linger.ms", 0)

			producerP1, err := NewProducer(configP1)
			if err != nil {
				t.Fatalf("Failed to create producer P1: %v", err)
			}
			defer func() { _ = producerP1.Close() }()

			txProducerP1 := producerP1.(kafka.TransactionalProducer)
			ctx := context.Background()

			// ============================================================
			// Step 2: P1 InitTransactions
			// ============================================================
			err = txProducerP1.InitTransactions(ctx)
			if err != nil {
				t.Fatalf("P1 InitTransactions failed: %v", err)
			}
			t.Log("P1: InitTransactions succeeded")

			// ============================================================
			// Step 3: P1 BeginTransaction
			// ============================================================
			err = txProducerP1.BeginTransaction()
			if err != nil {
				t.Fatalf("P1 BeginTransaction failed: %v", err)
			}
			t.Log("P1: BeginTransaction succeeded")

			// ============================================================
			// Step 4: P1 produces a message
			// ============================================================
			messageKey := fmt.Sprintf("p1-key-%d", time.Now().UnixNano())
			messageValue := "p1-test-value"
			record := txProducerP1.NewRecord(ctx, []byte(messageKey), []byte(messageValue), topicName, -1, time.Now(), nil, "")

			err = txProducerP1.ProduceAsync(ctx, record)
			if err != nil {
				t.Fatalf("P1 ProduceAsync failed: %v", err)
			}
			t.Log("P1: ProduceAsync succeeded (message queued)")

			// Flush and commit the first transaction
			txProducerP1.(*TransactionalProducer).Producer.Flush()
			t.Log("P1: Flush completed")

			err = txProducerP1.CommitTransaction(ctx)
			if err != nil {
				t.Fatalf("P1 CommitTransaction (first) failed: %v", err)
			}
			t.Log("P1: First transaction committed successfully")

			// ============================================================
			// Step 5: Create Producer P2 with SAME transactional.id
			// This will fence P1 when P2 calls InitTransactions
			// ============================================================
			t.Log("Creating P2 with same transactional.id to fence P1...")

			configP2 := NewProducerConfig()
			configP2.Id = "producer-p2"
			configP2.BootstrapServers = cluster.BootstrapServers()
			configP2.Logger = testLogger
			configP2.MetricsReporter = metrics.NoopReporter()
			configP2.Transactional.Enabled = true
			configP2.Transactional.Id = sharedTxId // Same transactional.id!
			_ = configP2.Librd.SetKey("transaction.timeout.ms", 10000)
			_ = configP2.Librd.SetKey("linger.ms", 0)

			producerP2, err := NewProducer(configP2)
			if err != nil {
				t.Fatalf("Failed to create producer P2: %v", err)
			}
			defer func() { _ = producerP2.Close() }()

			txProducerP2 := producerP2.(kafka.TransactionalProducer)

			// ============================================================
			// Step 6: P2 InitTransactions - this fences P1
			// ============================================================
			err = txProducerP2.InitTransactions(ctx)
			if err != nil {
				t.Fatalf("P2 InitTransactions failed: %v", err)
			}
			t.Log("P2: InitTransactions succeeded - P1 is now FENCED")

			// Give the broker a moment to propagate the fencing
			time.Sleep(500 * time.Millisecond)

			// ============================================================
			// Step 7: P1 attempts an operation - should fail due to fencing
			// ============================================================
			var fencingErr error

			// P1 attempts to start a new transaction - should fail because P1 is fenced
			fencingErr = txProducerP1.BeginTransaction()
			if fencingErr == nil {
				// If BeginTransaction succeeded, the next operation should fail
				record2 := txProducerP1.NewRecord(ctx, []byte("p1-key-2"), []byte("p1-value-2"), topicName, -1, time.Now(), nil, "")
				fencingErr = txProducerP1.ProduceAsync(ctx, record2)
				if fencingErr == nil {
					txProducerP1.(*TransactionalProducer).Producer.Flush()
					fencingErr = txProducerP1.CommitTransaction(ctx)
				}
			}

			// ============================================================
			// Step 8: Assert error expectations
			// ============================================================
			if tt.expectedBehavior.p1ErrorExpected {
				if fencingErr == nil {
					t.Errorf("Expected P1 to receive fencing error, but operation succeeded")
				} else {
					t.Logf("P1 received expected error: %v", fencingErr)

					producerErr, ok := fencingErr.(kafka.ProducerErr)
					if !ok {
						t.Errorf("Expected kafka.ProducerErr, got %T: %v", fencingErr, fencingErr)
					} else {
						if tt.expectedBehavior.p1ErrorRequiresRestart {
							if !producerErr.RequiresRestart() {
								t.Errorf("Expected RequiresRestart()=true, got false. Error: %v", fencingErr)
							} else {
								t.Log("P1 error correctly indicates RequiresRestart=true")
							}
						}

						t.Logf("Error classification: ShouldShutdown=%v, RequiresRestart=%v, TxnRequiresAbort=%v",
							producerErr.ShouldShutdown(), producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
					}
				}
			} else {
				if fencingErr != nil {
					t.Errorf("Expected P1 operation to succeed, but got error: %v", fencingErr)
				}
			}

			// ============================================================
			// Step 9: Verify message visibility
			// ============================================================
			time.Sleep(500 * time.Millisecond)
			verifyTimeout := 5 * time.Second

			// Check with read_uncommitted
			foundUncommitted, _ := mocks.ConsumeMessageReadUncommitted(t, cluster.BootstrapServers(), topicName, messageKey, verifyTimeout)
			if tt.expectedBehavior.p1MessageVisibleUncommitted {
				if !foundUncommitted {
					t.Errorf("Expected P1 message to be visible with read_uncommitted, but it was NOT found")
				} else {
					t.Log("P1 message correctly visible with read_uncommitted")
				}
			} else {
				if foundUncommitted {
					t.Errorf("Expected P1 message to NOT be visible with read_uncommitted, but it WAS found")
				} else {
					t.Log("P1 message correctly NOT visible with read_uncommitted")
				}
			}

			// Check with read_committed
			foundCommitted, _ := mocks.ConsumeMessageReadCommitted(t, cluster.BootstrapServers(), topicName, messageKey, verifyTimeout)
			if tt.expectedBehavior.p1MessageVisibleCommitted {
				if !foundCommitted {
					t.Errorf("Expected P1 message to be visible with read_committed, but it was NOT found")
				} else {
					t.Log("P1 message correctly visible with read_committed (transaction was committed before fencing)")
				}
			} else {
				if foundCommitted {
					t.Errorf("Expected P1 message to NOT be visible with read_committed, but it WAS found")
				} else {
					t.Log("P1 message correctly NOT visible with read_committed (transaction was aborted due to fencing)")
				}
			}
		})
	}
}

// =============================================================================
// TestTransactionalProducer_AbortBeforeMessageLeavesQueue tests the scenario where:
// 1. InitTransactions() succeeds
// 2. ProduceAsync() enqueues a message to librdkafka's local buffer
// 3. AbortTransaction() is called BEFORE the message is sent to the broker
//
// This tests that librdkafka correctly purges unsent messages from the local
// queue when a transaction is aborted, and that the message never reaches Kafka.
//
// The test uses a proxy to observe exactly which Kafka protocol messages are
// sent to the broker, verifying that no Produce request is sent before abort.
// =============================================================================

func TestTransactionalProducer_AbortBeforeMessageLeavesQueue(t *testing.T) {
	testLogger := mocks.NewTestLogger(t)

	// Setup Kafka with proxy to observe protocol traffic
	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
		mocks.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka cluster: %v", err)
	}
	defer cluster.Terminate(context.Background())

	topicName := "test-abort-before-send"
	cluster.CreateTopic(t, topicName, 1)

	proxy := cluster.Proxy()

	tests := []struct {
		name                         string
		lingerMs                     int  // How long messages stay in local queue before sending
		useExplicitBegin             bool // Whether to call BeginTransaction explicitly
		expectedAbortError           bool // Whether AbortTransaction should return an error
		expectedMessageInUncommitted bool // Message should NOT be visible (never sent)
		expectedMessageInCommitted   bool // Message should NOT be visible
	}{
		{
			// Note: linger.ms must be less than message.timeout.ms (default 300000ms)
			// Using 10 seconds which is long enough for the test but within limits
			name:                         "AbortWithLinger_AutoBegin_MessageNeverSent",
			lingerMs:                     10000, // 10 seconds - message stays in local queue
			useExplicitBegin:             false, // Let ProduceAsync auto-begin
			expectedAbortError:           false,
			expectedMessageInUncommitted: false, // Message never reached broker
			expectedMessageInCommitted:   false,
		},
		{
			name:                         "AbortWithLinger_ExplicitBegin_MessageNeverSent",
			lingerMs:                     10000,
			useExplicitBegin:             true, // Explicit BeginTransaction
			expectedAbortError:           false,
			expectedMessageInUncommitted: false,
			expectedMessageInCommitted:   false,
		},
		{
			// Shorter linger to verify abort works even with shorter queue time
			name:                         "AbortWithShortLinger_MessageNeverSent",
			lingerMs:                     5000, // 5 seconds
			useExplicitBegin:             false,
			expectedAbortError:           false,
			expectedMessageInUncommitted: false,
			expectedMessageInCommitted:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset proxy state and enable verbose logging for this test
			proxy.ClearErrorInjection()
			proxy.ClearDropRules()
			proxy.ClearRequestDropRules()
			proxy.SetLogger(testLogger)
			proxy.EnableVerboseLogging() // See all protocol traffic

			// Reset request counters to track what happens during this test
			proxy.ResetCounters()

			// Create producer with high linger.ms to keep messages in local queue
			config := NewProducerConfig()
			config.Id = "abort-before-send-test"
			config.BootstrapServers = cluster.BootstrapServers()
			config.Logger = testLogger
			config.MetricsReporter = metrics.NoopReporter()
			config.Transactional.Enabled = true
			config.Transactional.Id = uuid.New().String()

			// KEY CONFIG: High linger.ms keeps messages in local queue
			// Messages won't be sent to broker until linger.ms expires or Flush() is called
			_ = config.Librd.SetKey("linger.ms", tt.lingerMs)
			_ = config.Librd.SetKey("transaction.timeout.ms", 60000) // Long timeout to avoid interference

			producer, err := NewProducer(config)
			if err != nil {
				t.Fatalf("Failed to create producer: %v", err)
			}
			defer func() { _ = producer.Close() }()

			txProducer := producer.(kafka.TransactionalProducer)
			ctx := context.Background()

			// Step 1: InitTransactions
			err = txProducer.InitTransactions(ctx)
			if err != nil {
				t.Fatalf("InitTransactions failed: %v", err)
			}
			t.Log("InitTransactions succeeded")

			// Reset counters AFTER InitTransactions to only track produce-related traffic
			proxy.ResetCounters()
			t.Log("=== Proxy counters reset - tracking from here ===")

			// Step 2: BeginTransaction (explicit or via ProduceAsync)
			if tt.useExplicitBegin {
				err = txProducer.BeginTransaction()
				if err != nil {
					t.Fatalf("BeginTransaction failed: %v", err)
				}
				t.Log("BeginTransaction (explicit) succeeded")
			}

			// Step 3: ProduceAsync - message goes to LOCAL QUEUE only (not sent due to high linger.ms)
			messageKey := fmt.Sprintf("abort-test-key-%d", time.Now().UnixNano())
			messageValue := "this-message-should-never-reach-kafka"
			record := txProducer.NewRecord(ctx, []byte(messageKey), []byte(messageValue), topicName, -1, time.Now(), nil, "")

			err = txProducer.ProduceAsync(ctx, record)
			if err != nil {
				t.Fatalf("ProduceAsync failed: %v", err)
			}
			t.Logf("ProduceAsync succeeded - message '%s' is in LOCAL QUEUE (not sent yet due to linger.ms=%d)", messageKey, tt.lingerMs)

			// Check proxy counters BEFORE abort - should have NO Produce requests yet
			produceCountBefore := proxy.GetRequestCount(proxyPkg.APIKeyProduce)
			addPartitionsTxnCountBefore := proxy.GetRequestCount(proxyPkg.APIKeyAddPartitionsToTxn)
			t.Logf("BEFORE ABORT - Produce requests: %d, AddPartitionsToTxn requests: %d",
				produceCountBefore, addPartitionsTxnCountBefore)

			// Step 4: Immediately abort - BEFORE linger.ms expires, BEFORE message is sent to broker
			// This should purge the message from the local queue
			t.Log("Calling AbortTransaction immediately (before message leaves local queue)...")
			abortStart := time.Now()
			err = txProducer.AbortTransaction(ctx)
			abortDuration := time.Since(abortStart)

			// Check proxy counters AFTER abort
			produceCountAfter := proxy.GetRequestCount(proxyPkg.APIKeyProduce)
			addPartitionsTxnCountAfter := proxy.GetRequestCount(proxyPkg.APIKeyAddPartitionsToTxn)
			endTxnCount := proxy.GetRequestCount(proxyPkg.APIKeyEndTxn)
			t.Logf("AFTER ABORT - Produce requests: %d, AddPartitionsToTxn requests: %d, EndTxn requests: %d",
				produceCountAfter, addPartitionsTxnCountAfter, endTxnCount)

			if tt.expectedAbortError {
				if err == nil {
					t.Errorf("Expected AbortTransaction to fail, but it succeeded")
				} else {
					t.Logf("AbortTransaction failed as expected: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected AbortTransaction to succeed, but got error: %v", err)
				} else {
					t.Logf("AbortTransaction succeeded in %v", abortDuration)
				}
			}

			// Step 5: Verify NO Produce request was sent (message never left local queue)
			t.Log("=== PROTOCOL VERIFICATION ===")
			if produceCountAfter == 0 {
				t.Log("SUCCESS: No Produce requests sent - message stayed in local queue")
			} else {
				t.Errorf("UNEXPECTED: %d Produce request(s) sent - message may have left local queue!", produceCountAfter)
			}

			if addPartitionsTxnCountAfter == 0 {
				t.Log("SUCCESS: No AddPartitionsToTxn requests sent - partition not added to transaction")
			} else {
				t.Logf("INFO: %d AddPartitionsToTxn request(s) sent (this may happen if BeginTransaction triggers it)", addPartitionsTxnCountAfter)
			}

			// EndTxn might or might not be sent depending on whether BeginTransaction was called
			// and whether librdkafka tracks the transaction state
			t.Logf("INFO: %d EndTxn request(s) sent", endTxnCount)

			// Step 6: Verify message visibility
			time.Sleep(500 * time.Millisecond)

			verifyTimeout := 5 * time.Second

			// Check with read_uncommitted - message should NOT be found (never sent to broker)
			t.Log("Checking message visibility with read_uncommitted...")
			foundUncommitted, _ := mocks.ConsumeMessageReadUncommitted(t, cluster.BootstrapServers(), topicName, messageKey, verifyTimeout)

			if tt.expectedMessageInUncommitted {
				if !foundUncommitted {
					t.Errorf("Expected message to be visible with read_uncommitted, but it was NOT found")
				} else {
					t.Log("Message found with read_uncommitted (as expected)")
				}
			} else {
				if foundUncommitted {
					t.Errorf("Message should NOT be visible with read_uncommitted (was never sent), but it WAS found!")
					t.Error("This indicates the message was sent to broker before abort - linger.ms may not be working as expected")
				} else {
					t.Log("SUCCESS: Message correctly NOT found with read_uncommitted (never sent to broker)")
				}
			}

			// Check with read_committed - message should NOT be found
			t.Log("Checking message visibility with read_committed...")
			foundCommitted, _ := mocks.ConsumeMessageReadCommitted(t, cluster.BootstrapServers(), topicName, messageKey, verifyTimeout)

			if tt.expectedMessageInCommitted {
				if !foundCommitted {
					t.Errorf("Expected message to be visible with read_committed, but it was NOT found")
				} else {
					t.Log("Message found with read_committed (as expected)")
				}
			} else {
				if foundCommitted {
					t.Errorf("Message should NOT be visible with read_committed, but it WAS found!")
				} else {
					t.Log("SUCCESS: Message correctly NOT found with read_committed")
				}
			}

			// Step 7: Verify producer state is clean - should be able to start a new transaction
			t.Log("Verifying producer state is clean after abort...")
			err = txProducer.BeginTransaction()
			if err != nil {
				t.Errorf("Expected to start new transaction after abort, but got error: %v", err)
			} else {
				t.Log("SUCCESS: Producer state is clean - new transaction started successfully")
				// Abort this transaction to clean up
				_ = txProducer.AbortTransaction(ctx)
			}

			proxy.DisableVerboseLogging()
		})
	}
}
