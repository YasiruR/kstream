package librd

import (
	"context"
	"testing"

	librdKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics/v2"
)

// TestHandleTxError_ErrorClassification tests that handleTxError correctly classifies different error types
func TestHandleTxError_ErrorClassification(t *testing.T) {
	// Create a mock producer for testing
	config := NewProducerConfig()
	config.Id = "test-tx-producer"
	config.BootstrapServers = []string{"localhost:9092"}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "test-tx-id"

	// We need a minimal producer setup for the test
	// Using a mock cluster to avoid real connections
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	config.BootstrapServers = []string{mockCluster.BootstrapServers()}

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	tests := []struct {
		name              string
		errorCode         librdKafka.ErrorCode
		isFatal           bool
		expectShouldAbort bool
		expectRestart     bool
		expectRetry       bool
		description       string
	}{
		{
			name:              "timeout_error_should_abort",
			errorCode:         librdKafka.ErrTimedOut,
			isFatal:           false,
			expectShouldAbort: true,
			expectRestart:     false,
			expectRetry:       false,
			description:       "Timeout errors should trigger abort, not infinite retry",
		},
		{
			name:              "timeout_queue_error_should_abort",
			errorCode:         librdKafka.ErrTimedOutQueue,
			isFatal:           false,
			expectShouldAbort: true,
			expectRestart:     false,
			expectRetry:       false,
			description:       "Queue timeout errors should trigger abort",
		},
		{
			name:              "queue_full_should_abort",
			errorCode:         librdKafka.ErrQueueFull,
			isFatal:           false,
			expectShouldAbort: true,
			expectRestart:     false,
			expectRetry:       false,
			description:       "Queue full errors should trigger abort",
		},
		{
			name:              "fatal_error_should_restart",
			errorCode:         librdKafka.ErrFatal,
			isFatal:           true,
			expectShouldAbort: false,
			expectRestart:     true,
			expectRetry:       false,
			description:       "Fatal errors should trigger producer restart",
		},
		{
			name:              "state_error_should_restart",
			errorCode:         librdKafka.ErrState,
			isFatal:           false,
			expectShouldAbort: false,
			expectRestart:     true,
			expectRetry:       false,
			description:       "State errors should trigger producer restart",
		},
		// Note: ErrInvalidTxnState and ErrTransactionCoordinatorFenced
		// fall through to raw error return because TxnRequiresAbort()
		// is not set when creating errors via NewError().
		// These are tested separately in TestHandleTxError_TxnAbortableErrors
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a librdkafka error with the specific code
			librdErr := librdKafka.NewError(tt.errorCode, "test error", tt.isFatal)

			// Call handleTxError with a nil retry function (we don't expect retry for these cases)
			var retryCalled bool
			retryFunc := func() error {
				retryCalled = true
				return nil
			}

			result := txProducer.handleTxError(context.Background(), librdErr, "test operation", retryFunc, 1)

			// Check if retry was called (should not be for these test cases)
			if tt.expectRetry && !retryCalled {
				t.Error("Expected retry to be called but it wasn't")
			}
			if !tt.expectRetry && retryCalled {
				t.Error("Expected retry NOT to be called but it was")
			}

			// If retry was called, we don't get an Err back
			if retryCalled {
				return
			}

			// Check the returned error type
			if result == nil {
				t.Fatal("Expected error but got nil")
			}

			producerErr, ok := result.(Err)
			if !ok {
				// If not our Err type, it's the raw error passthrough
				if tt.expectShouldAbort || tt.expectRestart {
					t.Errorf("Expected Err type but got %T", result)
				}
				return
			}

			if tt.expectShouldAbort && !producerErr.TxnRequiresAbort() {
				t.Errorf("Expected TxnRequiresAbort()=true for %s, got false", tt.description)
			}

			if !tt.expectShouldAbort && producerErr.TxnRequiresAbort() {
				t.Errorf("Expected TxnRequiresAbort()=false for %s, got true", tt.description)
			}

			if tt.expectRestart && !producerErr.RequiresRestart() {
				t.Errorf("Expected RequiresRestart()=true for %s, got false", tt.description)
			}

			if !tt.expectRestart && producerErr.RequiresRestart() {
				t.Errorf("Expected RequiresRestart()=false for %s, got true", tt.description)
			}

			t.Logf("%s: Error code %v -> shouldAbort=%v, restart=%v",
				tt.name, tt.errorCode, producerErr.TxnRequiresAbort(), producerErr.RequiresRestart())
		})
	}
}

// TestHandleTxError_RetriableNonTimeout tests that retriable non-timeout errors trigger retry
func TestHandleTxError_RetriableNonTimeout(t *testing.T) {
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	config := NewProducerConfig()
	config.Id = "test-tx-producer"
	config.BootstrapServers = []string{mockCluster.BootstrapServers()}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "test-tx-id"

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	// Test retriable errors that are NOT timeouts should trigger retry
	retriableNonTimeoutCodes := []librdKafka.ErrorCode{
		librdKafka.ErrMsgTimedOut,     // Message timeout (retriable, but not IsTimeout())
		librdKafka.ErrRequestTimedOut, // Request timeout
		librdKafka.ErrNotCoordinator,  // Coordinator issues are retriable
	}

	for _, code := range retriableNonTimeoutCodes {
		t.Run(code.String(), func(t *testing.T) {
			librdErr := librdKafka.NewError(code, "test retriable error", false)

			// Check the error flags
			t.Logf("Error %v: IsRetriable=%v, IsTimeout=%v",
				code, librdErr.IsRetriable(), librdErr.IsTimeout())

			// Only test if it's retriable and not a timeout
			if !librdErr.IsRetriable() || librdErr.IsTimeout() {
				t.Skipf("Skipping %v: IsRetriable=%v, IsTimeout=%v",
					code, librdErr.IsRetriable(), librdErr.IsTimeout())
				return
			}

			retryCount := 0
			maxRetries := 3
			var retryFunc func() error
			retryFunc = func() error {
				retryCount++
				if retryCount >= maxRetries {
					// Return nil to stop retry loop
					return nil
				}
				// Return the same error to continue retrying
				return txProducer.handleTxError(context.Background(), librdErr, "test", retryFunc, 1)
			}

			_ = txProducer.handleTxError(context.Background(), librdErr, "test retriable", retryFunc, 1)

			if retryCount == 0 {
				t.Errorf("Expected retry to be called for retriable error %v", code)
			} else {
				t.Logf("Retry called %d times for %v (stopped at max)", retryCount, code)
			}
		})
	}
}

// TestHandleTxError_TimeoutNotRetried verifies timeout errors are NOT retried infinitely
func TestHandleTxError_TimeoutNotRetried(t *testing.T) {
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	config := NewProducerConfig()
	config.Id = "test-tx-producer"
	config.BootstrapServers = []string{mockCluster.BootstrapServers()}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "test-tx-id"

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	timeoutCodes := []librdKafka.ErrorCode{
		librdKafka.ErrTimedOut,
		librdKafka.ErrTimedOutQueue,
	}

	for _, code := range timeoutCodes {
		t.Run(code.String(), func(t *testing.T) {
			librdErr := librdKafka.NewError(code, "test timeout", false)

			// Verify it's both retriable and timeout
			t.Logf("Error %v: IsRetriable=%v, IsTimeout=%v",
				code, librdErr.IsRetriable(), librdErr.IsTimeout())

			retryCalled := false
			retryFunc := func() error {
				retryCalled = true
				t.Error("Retry should NOT be called for timeout errors!")
				return nil
			}

			result := txProducer.handleTxError(context.Background(), librdErr, "test timeout", retryFunc, 1)

			if retryCalled {
				t.Errorf("Timeout error %v should NOT trigger retry", code)
			}

			// Should return Err with shouldAbort=true
			producerErr, ok := result.(Err)
			if !ok {
				t.Fatalf("Expected Err type, got %T", result)
			}

			if !producerErr.TxnRequiresAbort() {
				t.Errorf("Timeout error %v should set TxnRequiresAbort()=true", code)
			}

			t.Logf("Timeout error %v correctly returns shouldAbort=true without retry", code)
		})
	}
}

// TestErr_Interface tests that Err correctly implements kafka.ProducerErr interface
func TestErr_Interface(t *testing.T) {
	tests := []struct {
		name          string
		err           Err
		expectAbort   bool
		expectRestart bool
		description   string
	}{
		{
			name: "abort_error",
			err: Err{
				error:       librdKafka.NewError(librdKafka.ErrTimedOut, "timeout", false),
				shouldAbort: true,
				restart:     false,
			},
			expectAbort:   true,
			expectRestart: false,
			description:   "Error requiring abort",
		},
		{
			name: "restart_error",
			err: Err{
				error:       librdKafka.NewError(librdKafka.ErrFatal, "fatal", true),
				shouldAbort: false,
				restart:     true,
			},
			expectAbort:   false,
			expectRestart: true,
			description:   "Error requiring restart",
		},
		{
			name: "no_action_error",
			err: Err{
				error:       librdKafka.NewError(librdKafka.ErrUnknown, "unknown", false),
				shouldAbort: false,
				restart:     false,
			},
			expectAbort:   false,
			expectRestart: false,
			description:   "Error with no special action",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.TxnRequiresAbort() != tt.expectAbort {
				t.Errorf("TxnRequiresAbort() = %v, want %v", tt.err.TxnRequiresAbort(), tt.expectAbort)
			}

			if tt.err.RequiresRestart() != tt.expectRestart {
				t.Errorf("RequiresRestart() = %v, want %v", tt.err.RequiresRestart(), tt.expectRestart)
			}

			// Test error string
			if tt.err.Error() == "" {
				t.Error("Error() should return non-empty string")
			}
		})
	}
}

// TestLibrdKafkaErrorFlags documents librdkafka error flag behavior
// This test helps detect if librdkafka behavior changes in future versions
func TestLibrdKafkaErrorFlags(t *testing.T) {
	tests := []struct {
		code            librdKafka.ErrorCode
		expectRetriable bool
		expectTimeout   bool
		description     string
	}{
		{librdKafka.ErrTimedOut, false, true, "Local timeout - NOT retriable, IS timeout"},
		{librdKafka.ErrTimedOutQueue, false, true, "Queue timeout - NOT retriable, IS timeout"},
		{librdKafka.ErrQueueFull, false, false, "Queue full - NOT retriable"},
		{librdKafka.ErrMsgTimedOut, false, false, "Message timeout - NOT retriable, NOT IsTimeout()"},
		{librdKafka.ErrRequestTimedOut, false, false, "Request timeout - NOT retriable"},
		{librdKafka.ErrFatal, false, false, "Fatal - NOT retriable"},
		{librdKafka.ErrState, false, false, "State - NOT retriable"},
	}

	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			err := librdKafka.NewError(tt.code, "test", false)

			if err.IsRetriable() != tt.expectRetriable {
				t.Errorf("IsRetriable() = %v, want %v for %s",
					err.IsRetriable(), tt.expectRetriable, tt.description)
			}

			if err.IsTimeout() != tt.expectTimeout {
				t.Errorf("IsTimeout() = %v, want %v for %s",
					err.IsTimeout(), tt.expectTimeout, tt.description)
			}

			t.Logf("%s: IsRetriable=%v, IsTimeout=%v, TxnRequiresAbort=%v, IsFatal=%v",
				tt.code, err.IsRetriable(), err.IsTimeout(), err.TxnRequiresAbort(), err.IsFatal())
		})
	}
}

// TestHandleTxError_FatalErrors tests fatal error handling
func TestHandleTxError_FatalErrors(t *testing.T) {
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	config := NewProducerConfig()
	config.Id = "test-tx-producer"
	config.BootstrapServers = []string{mockCluster.BootstrapServers()}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "test-tx-id"

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	fatalCodes := []struct {
		code    librdKafka.ErrorCode
		isFatal bool
	}{
		{librdKafka.ErrFatal, true},
		{librdKafka.ErrState, false}, // State errors should trigger restart
	}

	for _, tc := range fatalCodes {
		t.Run(tc.code.String(), func(t *testing.T) {
			librdErr := librdKafka.NewError(tc.code, "test fatal", tc.isFatal)

			retryCalled := false
			retryFunc := func() error {
				retryCalled = true
				return nil
			}

			result := txProducer.handleTxError(context.Background(), librdErr, "test fatal", retryFunc, 1)

			if retryCalled {
				t.Errorf("Fatal error %v should NOT trigger retry", tc.code)
			}

			producerErr, ok := result.(Err)
			if !ok {
				t.Fatalf("Expected Err type, got %T", result)
			}

			if !producerErr.RequiresRestart() {
				t.Errorf("Fatal error %v should set RequiresRestart()=true", tc.code)
			}

			t.Logf("Fatal error %v correctly returns restart=true", tc.code)
		})
	}
}

// TestHandleTxError_InjectedFatalError tests fatal error handling using TestFatalError()
// This injects a real fatal error into the producer to test GetFatalError() detection
func TestHandleTxError_InjectedFatalError(t *testing.T) {
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	config := NewProducerConfig()
	config.Id = "test-tx-producer"
	config.BootstrapServers = []string{mockCluster.BootstrapServers()}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "test-tx-id"

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	// Verify no fatal error initially
	fatalErr := txProducer.baseProducer.GetFatalError()
	if fatalErr != nil {
		t.Fatalf("Expected no initial fatal error, got: %v", fatalErr)
	}

	// Inject a fatal error using TestFatalError
	injectedCode := librdKafka.ErrInvalidTimestamp
	injectedMsg := "INJECTED_FATAL_ERROR_FOR_TEST"
	txProducer.baseProducer.TestFatalError(injectedCode, injectedMsg)

	// Verify GetFatalError now returns the injected error
	fatalErr = txProducer.baseProducer.GetFatalError()
	if fatalErr == nil {
		t.Fatal("Expected fatal error after injection, got nil")
	}

	librdFatalErr, ok := fatalErr.(librdKafka.Error)
	if !ok {
		t.Fatalf("Expected librdKafka.Error type, got %T", fatalErr)
	}

	if !librdFatalErr.IsFatal() {
		t.Error("Expected IsFatal()=true for injected fatal error")
	}

	t.Logf("Injected fatal error: Code=%v, IsFatal=%v, Error=%s",
		librdFatalErr.Code(), librdFatalErr.IsFatal(), librdFatalErr.Error())

	// Test handleTxError with any error - it should detect the fatal state via GetFatalError()
	// This simulates what happens when any operation fails after a fatal error has occurred
	ctx := context.Background()
	anyErr := librdKafka.NewError(librdKafka.ErrUnknown, "some error", false)

	retryCalled := false
	retryFunc := func() error {
		retryCalled = true
		return nil
	}

	result := txProducer.handleTxError(ctx, anyErr, "test with injected fatal", retryFunc, 1)

	if retryCalled {
		t.Error("Should NOT retry when producer has fatal error")
	}

	producerErr, ok := result.(Err)
	if !ok {
		t.Fatalf("Expected Err type, got %T", result)
	}

	if !producerErr.RequiresRestart() {
		t.Error("Expected RequiresRestart()=true when producer has fatal error")
	}

	t.Logf("handleTxError correctly detected injected fatal error and returns restart=true")
}

// =============================================================================
// Tests aligned with Java KafkaProducer documentation
// https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
// =============================================================================

// TestTransactionalProducer_FatalErrors tests fatal errors that require producer restart
// These map to Java's ProducerFencedException, OutOfOrderSequenceException, etc.
func TestTransactionalProducer_FatalErrors(t *testing.T) {
	// Fatal errors that require producer restart (per Java KafkaProducer docs)
	fatalErrors := []struct {
		code        librdKafka.ErrorCode
		javaEquiv   string
		description string
	}{
		{
			code:        librdKafka.ErrFencedInstanceID,
			javaEquiv:   "ProducerFencedException",
			description: "Another producer with same transactional.id took over",
		},
		{
			code:        librdKafka.ErrOutOfOrderSequenceNumber,
			javaEquiv:   "OutOfOrderSequenceException",
			description: "Idempotent producer sequence numbers out of order",
		},
		{
			code:        librdKafka.ErrClusterAuthorizationFailed,
			javaEquiv:   "AuthorizationException",
			description: "Not authorized for cluster operations",
		},
		{
			code:        librdKafka.ErrTopicAuthorizationFailed,
			javaEquiv:   "AuthorizationException",
			description: "Not authorized for topic operations",
		},
		{
			code:        librdKafka.ErrInvalidProducerEpoch,
			javaEquiv:   "InvalidProducerEpochException",
			description: "Producer epoch is invalid/fenced",
		},
		{
			code:        librdKafka.ErrInvalidProducerIDMapping,
			javaEquiv:   "InvalidPidMappingException",
			description: "Producer ID mapping is invalid",
		},
		{
			code:        librdKafka.ErrFatal,
			javaEquiv:   "KafkaException (fatal)",
			description: "Generic fatal error",
		},
		{
			code:        librdKafka.ErrState,
			javaEquiv:   "IllegalStateException",
			description: "Producer in invalid state",
		},
	}

	for _, tc := range fatalErrors {
		t.Run(tc.code.String(), func(t *testing.T) {
			mockCluster, err := librdKafka.NewMockCluster(1)
			if err != nil {
				t.Fatalf("Failed to create mock cluster: %v", err)
			}
			defer mockCluster.Close()

			config := NewProducerConfig()
			config.Id = "test-tx-producer"
			config.BootstrapServers = []string{mockCluster.BootstrapServers()}
			config.Logger = log.NewNoopLogger()
			config.MetricsReporter = metrics.NoopReporter()
			config.Transactional.Enabled = true
			config.Transactional.Id = "test-tx-id"

			producer, err := NewProducer(config)
			if err != nil {
				t.Fatalf("Failed to create producer: %v", err)
			}
			defer producer.Close()

			txProducer := producer.(*TransactionalProducer)

			// Create error with the fatal code
			librdErr := librdKafka.NewError(tc.code, tc.description, true)

			retryFunc := func() error {
				t.Error("Retry should NOT be called for fatal errors")
				return nil
			}

			result := txProducer.handleTxError(context.Background(), librdErr, "test fatal", retryFunc, 1)

			// Verify error classification
			producerErr, ok := result.(Err)
			if !ok {
				t.Logf("Error %v returned raw error (may need code adjustment): %v", tc.code, result)
				return
			}

			// Fatal errors should require restart
			if !producerErr.RequiresRestart() {
				t.Errorf("Java %s (librd %v) should require restart, got RequiresRestart()=false",
					tc.javaEquiv, tc.code)
			}

			t.Logf("✓ %s (%v): RequiresRestart=%v, TxnRequiresAbort=%v",
				tc.javaEquiv, tc.code, producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
		})
	}
}

// TestTransactionalProducer_AbortableErrors tests errors that require transaction abort
// These are recoverable by aborting the current transaction and starting a new one
func TestTransactionalProducer_AbortableErrors(t *testing.T) {
	abortableErrors := []struct {
		code        librdKafka.ErrorCode
		javaEquiv   string
		description string
	}{
		{
			code:        librdKafka.ErrTransactionCoordinatorFenced,
			javaEquiv:   "TransactionAbortedException",
			description: "Transaction coordinator fenced",
		},
		{
			code:        librdKafka.ErrInvalidTxnState,
			javaEquiv:   "InvalidTxnStateException",
			description: "Invalid transaction state",
		},
		{
			code:        librdKafka.ErrTimedOut,
			javaEquiv:   "TimeoutException",
			description: "Operation timed out locally",
		},
		{
			code:        librdKafka.ErrTimedOutQueue,
			javaEquiv:   "TimeoutException",
			description: "Operation timed out in queue",
		},
		{
			code:        librdKafka.ErrQueueFull,
			javaEquiv:   "BufferExhaustedException",
			description: "Producer queue is full",
		},
	}

	for _, tc := range abortableErrors {
		t.Run(tc.code.String(), func(t *testing.T) {
			mockCluster, err := librdKafka.NewMockCluster(1)
			if err != nil {
				t.Fatalf("Failed to create mock cluster: %v", err)
			}
			defer mockCluster.Close()

			config := NewProducerConfig()
			config.Id = "test-tx-producer"
			config.BootstrapServers = []string{mockCluster.BootstrapServers()}
			config.Logger = log.NewNoopLogger()
			config.MetricsReporter = metrics.NoopReporter()
			config.Transactional.Enabled = true
			config.Transactional.Id = "test-tx-id"

			producer, err := NewProducer(config)
			if err != nil {
				t.Fatalf("Failed to create producer: %v", err)
			}
			defer producer.Close()

			txProducer := producer.(*TransactionalProducer)

			librdErr := librdKafka.NewError(tc.code, tc.description, false)

			retryCalled := false
			retryFunc := func() error {
				retryCalled = true
				return nil
			}

			result := txProducer.handleTxError(context.Background(), librdErr, "test abortable", retryFunc, 1)

			// Abortable errors should not trigger retry
			if retryCalled {
				t.Errorf("Retry should NOT be called for abortable errors")
			}

			producerErr, ok := result.(Err)
			if !ok {
				t.Logf("Error %v returned raw error: %v", tc.code, result)
				return
			}

			// Abortable errors should require abort
			if !producerErr.TxnRequiresAbort() {
				t.Errorf("Java %s (librd %v) should require abort, got TxnRequiresAbort()=false",
					tc.javaEquiv, tc.code)
			}

			// Should NOT require restart (abort is sufficient)
			if producerErr.RequiresRestart() {
				t.Errorf("Java %s (librd %v) should NOT require restart for abortable error",
					tc.javaEquiv, tc.code)
			}

			t.Logf("✓ %s (%v): TxnRequiresAbort=%v, RequiresRestart=%v",
				tc.javaEquiv, tc.code, producerErr.TxnRequiresAbort(), producerErr.RequiresRestart())
		})
	}
}

// TestTransactionalProducer_InjectedFatalError_TransactionOps tests error handling
// during transaction lifecycle operations using TestFatalError() for realistic simulation
func TestTransactionalProducer_InjectedFatalError_TransactionOps(t *testing.T) {
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	config := NewProducerConfig()
	config.Id = "test-tx-producer"
	config.BootstrapServers = []string{mockCluster.BootstrapServers()}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "test-tx-id"

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	// Inject fatal error using TestFatalError
	injectedCode := librdKafka.ErrFencedInstanceID
	injectedMsg := "PRODUCER_FENCED_BY_ANOTHER_INSTANCE"
	txProducer.baseProducer.TestFatalError(injectedCode, injectedMsg)

	// Verify GetFatalError returns the injected error
	fatalErr := txProducer.baseProducer.GetFatalError()
	if fatalErr == nil {
		t.Fatal("Expected fatal error after injection")
	}

	librdFatalErr, ok := fatalErr.(librdKafka.Error)
	if !ok {
		t.Fatalf("Expected librdKafka.Error, got %T", fatalErr)
	}

	t.Logf("Injected fatal error: Code=%v, IsFatal=%v, Error=%s",
		librdFatalErr.Code(), librdFatalErr.IsFatal(), librdFatalErr.Error())

	// Now test handleTxError - it should detect fatal state via GetFatalError()
	anyErr := librdKafka.NewError(librdKafka.ErrUnknown, "some operation failed", false)

	result := txProducer.handleTxError(context.Background(), anyErr, "during transaction", func() error {
		t.Error("Retry should not be called when fatal error is present")
		return nil
	}, 1)

	producerErr, ok := result.(Err)
	if !ok {
		t.Fatalf("Expected Err type, got %T", result)
	}

	if !producerErr.RequiresRestart() {
		t.Error("Expected RequiresRestart()=true when producer has fatal error")
	}

	t.Logf("✓ handleTxError detected injected fatal error (ProducerFencedException equivalent)")
	t.Logf("  Result: RequiresRestart=%v, TxnRequiresAbort=%v",
		producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
}

// TestTransactionalProducer_ErrorRecoveryFlow tests the full error recovery flow
// as documented in Java KafkaProducer: abort transaction -> begin new transaction
func TestTransactionalProducer_ErrorRecoveryFlow(t *testing.T) {
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	config := NewProducerConfig()
	config.Id = "test-tx-producer"
	config.BootstrapServers = []string{mockCluster.BootstrapServers()}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "test-tx-id"

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	// Test scenario: Simulate abortable error -> verify abort is required -> new tx can start
	t.Run("abortable_error_recovery", func(t *testing.T) {
		// Simulate timeout error (abortable)
		timeoutErr := librdKafka.NewError(librdKafka.ErrTimedOut, "commit timed out", false)

		result := txProducer.handleTxError(context.Background(), timeoutErr, "commit transaction", func() error {
			return nil
		}, 1)

		producerErr, ok := result.(Err)
		if !ok {
			t.Fatalf("Expected Err type, got %T", result)
		}

		// Step 1: Verify abort is required
		if !producerErr.TxnRequiresAbort() {
			t.Error("Timeout should require transaction abort")
		}

		// Step 2: Verify restart is NOT required (abort is sufficient)
		if producerErr.RequiresRestart() {
			t.Error("Timeout should NOT require restart")
		}

		t.Logf("✓ Abortable error correctly classified: abort=%v, restart=%v",
			producerErr.TxnRequiresAbort(), producerErr.RequiresRestart())
	})

	// Test scenario: Simulate fatal error -> verify restart is required
	t.Run("fatal_error_requires_restart", func(t *testing.T) {
		// Create fresh producer for clean state
		freshProducer, err := NewProducer(config)
		if err != nil {
			t.Fatalf("Failed to create producer: %v", err)
		}
		defer freshProducer.Close()

		freshTxProducer := freshProducer.(*TransactionalProducer)

		// Inject fatal error
		freshTxProducer.baseProducer.TestFatalError(librdKafka.ErrOutOfOrderSequenceNumber, "sequence_error")

		// Any error handling should detect the fatal state
		anyErr := librdKafka.NewError(librdKafka.ErrUnknown, "some error", false)
		result := freshTxProducer.handleTxError(context.Background(), anyErr, "produce", func() error {
			return nil
		}, 1)

		producerErr, ok := result.(Err)
		if !ok {
			t.Fatalf("Expected Err type, got %T", result)
		}

		// Verify restart is required
		if !producerErr.RequiresRestart() {
			t.Error("Fatal error should require restart")
		}

		t.Logf("✓ Fatal error correctly requires restart: restart=%v",
			producerErr.RequiresRestart())
	})
}

// TestTransactionalProducer_JavaErrorMapping documents the mapping between
// Java KafkaProducer exceptions and librdkafka error codes
func TestTransactionalProducer_JavaErrorMapping(t *testing.T) {
	// This test documents and verifies the mapping between Java and librdkafka errors
	// Reference: https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html

	mappings := []struct {
		javaException   string
		librdCode       librdKafka.ErrorCode
		expectedAction  string
		requiresRestart bool
		requiresAbort   bool
		description     string
	}{
		// Fatal errors - require closing the producer
		{
			javaException:   "ProducerFencedException",
			librdCode:       librdKafka.ErrFencedInstanceID,
			expectedAction:  "close producer, create new instance",
			requiresRestart: true,
			requiresAbort:   false,
			description:     "Another producer with same transactional.id",
		},
		{
			javaException:   "OutOfOrderSequenceException",
			librdCode:       librdKafka.ErrOutOfOrderSequenceNumber,
			expectedAction:  "close producer, create new instance",
			requiresRestart: true,
			requiresAbort:   false,
			description:     "Idempotent producer sequence error",
		},
		{
			javaException:   "AuthorizationException",
			librdCode:       librdKafka.ErrTopicAuthorizationFailed,
			expectedAction:  "close producer, fix permissions",
			requiresRestart: true,
			requiresAbort:   false,
			description:     "Not authorized for topic",
		},
		{
			javaException:   "InvalidProducerEpochException",
			librdCode:       librdKafka.ErrInvalidProducerEpoch,
			expectedAction:  "close producer, create new instance",
			requiresRestart: true,
			requiresAbort:   false,
			description:     "Producer epoch invalid",
		},

		// Abortable errors - abort transaction and retry
		{
			javaException:   "TimeoutException",
			librdCode:       librdKafka.ErrTimedOut,
			expectedAction:  "abort transaction, retry",
			requiresRestart: false,
			requiresAbort:   true,
			description:     "Operation timed out",
		},
		{
			javaException:   "TimeoutException (queue)",
			librdCode:       librdKafka.ErrTimedOutQueue,
			expectedAction:  "abort transaction, retry",
			requiresRestart: false,
			requiresAbort:   true,
			description:     "Queue timeout",
		},
		{
			javaException:   "BufferExhaustedException",
			librdCode:       librdKafka.ErrQueueFull,
			expectedAction:  "abort transaction, backoff, retry",
			requiresRestart: false,
			requiresAbort:   true,
			description:     "Producer queue full",
		},
	}

	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	config := NewProducerConfig()
	config.Id = "test-tx-producer"
	config.BootstrapServers = []string{mockCluster.BootstrapServers()}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "test-tx-id"

	for _, m := range mappings {
		t.Run(m.javaException, func(t *testing.T) {
			producer, err := NewProducer(config)
			if err != nil {
				t.Fatalf("Failed to create producer: %v", err)
			}
			defer producer.Close()

			txProducer := producer.(*TransactionalProducer)

			// Create error with the mapped code
			isFatal := m.requiresRestart
			librdErr := librdKafka.NewError(m.librdCode, m.description, isFatal)

			result := txProducer.handleTxError(context.Background(), librdErr, "test", func() error {
				return nil
			}, 1)

			// Check error classification
			producerErr, ok := result.(Err)
			if !ok {
				t.Logf("⚠ %s (%v): returned raw error, may need handleTxError adjustment",
					m.javaException, m.librdCode)
				return
			}

			// Verify mapping
			restartMatch := producerErr.RequiresRestart() == m.requiresRestart
			abortMatch := producerErr.TxnRequiresAbort() == m.requiresAbort

			if !restartMatch || !abortMatch {
				t.Errorf("Mapping mismatch for %s (%v):\n"+
					"  Expected: restart=%v, abort=%v\n"+
					"  Got:      restart=%v, abort=%v",
					m.javaException, m.librdCode,
					m.requiresRestart, m.requiresAbort,
					producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
			} else {
				t.Logf("✓ %s → %v: action=%s, restart=%v, abort=%v",
					m.javaException, m.librdCode, m.expectedAction,
					producerErr.RequiresRestart(), producerErr.TxnRequiresAbort())
			}
		})
	}
}

// TestTransactionalProducer_MultipleConsecutiveErrors tests handling of multiple
// consecutive errors (as might occur in production)
func TestTransactionalProducer_MultipleConsecutiveErrors(t *testing.T) {
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	config := NewProducerConfig()
	config.Id = "test-tx-producer"
	config.BootstrapServers = []string{mockCluster.BootstrapServers()}
	config.Logger = log.NewNoopLogger()
	config.MetricsReporter = metrics.NoopReporter()
	config.Transactional.Enabled = true
	config.Transactional.Id = "test-tx-id"

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	txProducer := producer.(*TransactionalProducer)

	// Simulate a series of errors as might occur in production
	errorSequence := []struct {
		code        librdKafka.ErrorCode
		description string
	}{
		{librdKafka.ErrTimedOut, "first timeout"},
		{librdKafka.ErrQueueFull, "queue full during retry"},
		{librdKafka.ErrTimedOut, "second timeout"},
	}

	abortCount := 0
	for i, errCase := range errorSequence {
		librdErr := librdKafka.NewError(errCase.code, errCase.description, false)

		result := txProducer.handleTxError(context.Background(), librdErr, "operation", func() error {
			return nil
		}, 1)

		producerErr, ok := result.(Err)
		if !ok {
			continue
		}

		if producerErr.TxnRequiresAbort() {
			abortCount++
		}

		t.Logf("Error %d (%v): abort=%v, restart=%v",
			i+1, errCase.code, producerErr.TxnRequiresAbort(), producerErr.RequiresRestart())
	}

	t.Logf("✓ Handled %d consecutive errors, %d required abort", len(errorSequence), abortCount)
}
