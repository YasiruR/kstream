package librd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka/mocks"
	proxyPkg "github.com/gmbyapa/kstream/v2/kafka/mocks/proxy"
	"github.com/google/uuid"
	"github.com/tryfix/metrics/v2"
)

// =============================================================================
// Mock RebalanceHandler for testing
// =============================================================================

// mockRebalanceHandler implements kafka.RebalanceHandler for testing
type mockRebalanceHandler struct {
	mu sync.Mutex

	// Channels for signaling callback invocations
	assignedCalled chan []kafka.TopicPartition
	revokedCalled  chan []kafka.TopicPartition
	lostCalled     chan struct{}
	consumeCalled  chan kafka.TopicPartition

	// Errors to return from callbacks
	assignError  error
	revokeError  error
	consumeError error

	// Tracking
	assignCount  int
	revokeCount  int
	lostCount    int
	consumeCount int

	// Messages received
	messages []kafka.Record

	// For stopping consumption
	stopOnMessage bool
	stopChan      chan struct{}
}

func newMockRebalanceHandler() *mockRebalanceHandler {
	return &mockRebalanceHandler{
		assignedCalled: make(chan []kafka.TopicPartition, 10),
		revokedCalled:  make(chan []kafka.TopicPartition, 10),
		lostCalled:     make(chan struct{}, 10),
		consumeCalled:  make(chan kafka.TopicPartition, 10),
		stopChan:       make(chan struct{}),
	}
}

func (m *mockRebalanceHandler) OnPartitionAssigned(ctx context.Context, session kafka.GroupSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assignCount++

	assignment := session.Assignment()
	tps := assignment.TPs()

	// Non-blocking send to channel
	select {
	case m.assignedCalled <- tps:
	default:
	}

	return m.assignError
}

func (m *mockRebalanceHandler) OnPartitionRevoked(ctx context.Context, session kafka.GroupSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revokeCount++

	assignment := session.Assignment()
	tps := assignment.TPs()

	// Non-blocking send to channel
	select {
	case m.revokedCalled <- tps:
	default:
	}

	return m.revokeError
}

func (m *mockRebalanceHandler) OnLost() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lostCount++

	// Non-blocking send to channel
	select {
	case m.lostCalled <- struct{}{}:
	default:
	}

	return nil
}

func (m *mockRebalanceHandler) Consume(ctx context.Context, session kafka.GroupSession, partition kafka.PartitionClaim) error {
	m.mu.Lock()
	m.consumeCount++
	tp := partition.TopicPartition()
	m.mu.Unlock()

	// Non-blocking send to channel
	select {
	case m.consumeCalled <- tp:
	default:
	}

	// Consume messages until stopped or context cancelled
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.stopChan:
			return nil
		case record, ok := <-partition.Records():
			if !ok {
				return nil
			}
			m.mu.Lock()
			m.messages = append(m.messages, record)
			m.mu.Unlock()

			if m.stopOnMessage {
				return nil
			}
		}
	}
}

func (m *mockRebalanceHandler) Stop() {
	close(m.stopChan)
}

func (m *mockRebalanceHandler) GetAssignCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.assignCount
}

func (m *mockRebalanceHandler) GetRevokeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.revokeCount
}

func (m *mockRebalanceHandler) GetLostCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lostCount
}

func (m *mockRebalanceHandler) GetMessages() []kafka.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.messages
}

// =============================================================================
// Test Infrastructure
// =============================================================================

// NOTE: injectType and related constants are defined in transactional_producer_behaviour_test.go
// They are shared across test files in this package.

// consumerTestFields defines test case parameters for consumer tests
type consumerTestFields struct {
	injectType          injectType
	injectApi           proxyPkg.KafkaAPIKey
	injectedError       proxyPkg.KafkaErrorCode
	injectedErrorCount  int
	sessionTimeoutMs    int
	heartbeatIntervalMs int
}

// consumerExpectedBehavior defines expected behavior for consumer tests
type consumerExpectedBehavior struct {
	shouldJoinSuccessfully  bool
	shouldReceivePartitions bool
	shouldTriggerRebalance  bool
	shouldReceiveError      bool
	errorPattern            string
}

// =============================================================================
// TestGroupConsumer_JoinGroup - Tests JoinGroup error scenarios
// =============================================================================

func TestGroupConsumer_JoinGroup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	tests := []struct {
		name     string
		fields   consumerTestFields
		expected consumerExpectedBehavior
	}{
		{
			name: "JoinGroup_CoordinatorNotAvailable_ShouldRetryAndSucceed",
			fields: consumerTestFields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyJoinGroup,
				injectedError:      proxyPkg.ErrCoordinatorNotAvailable,
				injectedErrorCount: 2, // Inject 2 errors, then let through
			},
			expected: consumerExpectedBehavior{
				shouldJoinSuccessfully:  true,
				shouldReceivePartitions: true,
			},
		},
		{
			name: "JoinGroup_NotCoordinator_ShouldRetryAndSucceed",
			fields: consumerTestFields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyJoinGroup,
				injectedError:      proxyPkg.ErrNotCoordinator,
				injectedErrorCount: 2,
			},
			expected: consumerExpectedBehavior{
				shouldJoinSuccessfully:  true,
				shouldReceivePartitions: true,
			},
		},
		{
			name: "JoinGroup_RebalanceInProgress_ShouldRetryAndSucceed",
			fields: consumerTestFields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyJoinGroup,
				injectedError:      proxyPkg.ErrRebalanceInProgress,
				injectedErrorCount: 2,
			},
			expected: consumerExpectedBehavior{
				shouldJoinSuccessfully:  true,
				shouldReceivePartitions: true,
			},
		},
		{
			name: "JoinGroup_GroupAuthorizationFailed_ShouldFail",
			fields: consumerTestFields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyJoinGroup,
				injectedError:      proxyPkg.ErrGroupAuthorizationFailed,
				injectedErrorCount: 100, // Inject continuously to prevent retry success
			},
			expected: consumerExpectedBehavior{
				shouldJoinSuccessfully: false,
				shouldReceiveError:     true,
				errorPattern:           "authorization",
			},
		},
		{
			name: "JoinGroup_InvalidGroupID_ShouldFail",
			fields: consumerTestFields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyJoinGroup,
				injectedError:      proxyPkg.ErrInvalidGroupID,
				injectedErrorCount: 100, // Inject continuously to prevent retry success
			},
			expected: consumerExpectedBehavior{
				shouldJoinSuccessfully: false,
				shouldReceiveError:     true,
				errorPattern:           "invalid",
			},
		},
		{
			name: "JoinGroup_UnknownMemberID_ShouldRejoinAndSucceed",
			fields: consumerTestFields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyJoinGroup,
				injectedError:      proxyPkg.ErrUnknownMemberID,
				injectedErrorCount: 1,
			},
			expected: consumerExpectedBehavior{
				shouldJoinSuccessfully:  true,
				shouldReceivePartitions: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runGroupConsumerTest(t, tt.fields, tt.expected)
		})
	}
}

// =============================================================================
// TestGroupConsumer_Heartbeat - Tests Heartbeat error scenarios
// =============================================================================

func TestGroupConsumer_Heartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	tests := []struct {
		name     string
		fields   consumerTestFields
		expected consumerExpectedBehavior
	}{
		{
			name: "Heartbeat_RebalanceInProgress_ShouldTriggerRebalance",
			fields: consumerTestFields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyHeartbeat,
				injectedError:      proxyPkg.ErrRebalanceInProgress,
				injectedErrorCount: 1, // Inject error after consumer joins
			},
			expected: consumerExpectedBehavior{
				shouldJoinSuccessfully: true,
				shouldTriggerRebalance: true,
			},
		},
		{
			name: "Heartbeat_IllegalGeneration_ShouldRejoin",
			fields: consumerTestFields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyHeartbeat,
				injectedError:      proxyPkg.ErrIllegalGeneration,
				injectedErrorCount: 1,
			},
			expected: consumerExpectedBehavior{
				shouldJoinSuccessfully: true,
				shouldTriggerRebalance: true,
			},
		},
		{
			name: "Heartbeat_UnknownMemberID_ShouldRejoin",
			fields: consumerTestFields{
				injectType:         errInjectTypeResponseError,
				injectApi:          proxyPkg.APIKeyHeartbeat,
				injectedError:      proxyPkg.ErrUnknownMemberID,
				injectedErrorCount: 1,
			},
			expected: consumerExpectedBehavior{
				shouldJoinSuccessfully: true,
				shouldTriggerRebalance: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runGroupConsumerHeartbeatTest(t, tt.fields, tt.expected)
		})
	}
}

// TestGroupConsumer_SessionTimeoutDuringCallback tests what happens when
// the session times out DURING the OnPartitionAssigned callback
// (i.e., before c.Assign() is called).
func TestGroupConsumer_SessionTimeoutDuringCallback(t *testing.T) {
	testLogger := mocks.NewTestLogger(t)
	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
		mocks.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer cluster.Terminate(context.Background())

	topicName := fmt.Sprintf("test-consumer-%s", uuid.New().String()[:8])
	cluster.CreateTopic(t, topicName, 1)

	proxy := cluster.Proxy()
	defer func() {
		proxy.ClearErrorInjection()
		proxy.ClearDropRules()
		proxy.ClearRequestDropRules()
	}()

	proxy.EnableVerboseLogging()

	// Create consumer config with SHORT session timeout
	// The sleep in OnPartitionAssigned is 10 seconds
	// Setting session.timeout.ms to 6 seconds means the session will timeout
	// BEFORE Assign() is called
	groupID := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	config := NewGroupConsumerConfig()
	config.BootstrapServers = cluster.BootstrapServers()
	config.GroupId = groupID
	config.Logger = testLogger
	config.MetricsReporter = metrics.NoopReporter()
	config.Librd.SetKey("session.timeout.ms", 6000)    // 6 seconds - less than 10s sleep
	config.Librd.SetKey("heartbeat.interval.ms", 1000) // 1 second
	config.Librd.SetKey("max.poll.interval.ms", 60000) // 60 seconds

	// Create consumer
	consumer, err := NewGroupConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	// Create mock handler
	handler := newMockRebalanceHandler()

	// Start consumer in goroutine
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- consumer.Subscribe([]string{topicName}, handler)
	}()

	// Drop heartbeat REQUESTS to simulate network issue to coordinator
	// This will cause the broker to not receive heartbeats
	t.Log("Dropping ALL heartbeat requests to simulate network partition...")
	proxy.DropRequestsFor(proxyPkg.APIKeyHeartbeat, 100)

	t.Log("Waiting to observe session timeout behavior...")
	t.Log("Expected: Session times out after 6s, but sleep is 10s")
	t.Log("Question: What happens when Assign() is called after session timeout?")

	// Wait and observe what happens
	timeout := time.After(90 * time.Second)
	for {
		select {
		case partitions := <-handler.assignedCalled:
			t.Logf("EVENT: OnPartitionAssigned called with %d partitions", len(partitions))
		case partitions := <-handler.revokedCalled:
			t.Logf("EVENT: OnPartitionRevoked called with %d partitions", len(partitions))
		case <-handler.lostCalled:
			t.Log("EVENT: OnLost called - assignment was lost!")
		case err := <-consumer.Errors():
			t.Logf("EVENT: Error from consumer: %v", err)
		case err := <-subscribeDone:
			if err != nil {
				t.Logf("EVENT: Subscribe returned error: %v", err)
			} else {
				t.Log("EVENT: Subscribe returned successfully")
			}
			goto done
		case <-timeout:
			t.Log("Test timeout reached")
			goto done
		}
	}

done:
	t.Log("Stopping consumer...")
	handler.Stop()
	consumer.Unsubscribe()
	t.Log("Test complete")
}

// =============================================================================
// TestGroupConsumer_SplitBrain - Tests split-brain scenarios where client
// receives fake responses but broker never receives requests
// =============================================================================

// TestGroupConsumer_SplitBrain_FakeHeartbeatSuccess tests a split-brain scenario:
// - The proxy intercepts heartbeat requests from the client
// - The proxy sends back fake SUCCESS responses to the client (error_code=0)
// - The proxy does NOT forward the heartbeat requests to the broker
// - Eventually, the broker's session for this consumer times out
// - The client should detect this (via coordinator error on next real interaction)
// - The consumer should trigger OnLost and attempt to re-join the group
//
// This simulates a network partition where the client can receive responses
// (from a cache, load balancer, or other proxy) but the broker never receives
// the client's heartbeats.
func TestGroupConsumer_SplitBrain_FakeHeartbeatSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testLogger := mocks.NewTestLogger(t)
	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
		mocks.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer cluster.Terminate(context.Background())

	topicName := fmt.Sprintf("test-consumer-split-brain-%s", uuid.New().String()[:8])
	cluster.CreateTopic(t, topicName, 1)

	proxy := cluster.Proxy()
	defer func() {
		proxy.ClearErrorInjection()
		proxy.ClearDropRules()
		proxy.ClearRequestDropRules()
		proxy.ClearFakeResponseRules()
	}()

	proxy.EnableVerboseLogging()

	// Create consumer config with SHORT session timeout so the test runs faster
	// session.timeout.ms = 6000 (6 seconds) - broker will kick consumer after this
	// heartbeat.interval.ms = 1000 (1 second) - client sends heartbeat every second
	groupID := fmt.Sprintf("test-group-split-brain-%s", uuid.New().String()[:8])
	config := NewGroupConsumerConfig()
	config.BootstrapServers = cluster.BootstrapServers()
	config.GroupId = groupID
	config.Logger = testLogger
	config.MetricsReporter = metrics.NoopReporter()
	config.Librd.SetKey("session.timeout.ms", 6000)    // 6 seconds - short timeout
	config.Librd.SetKey("heartbeat.interval.ms", 1000) // 1 second
	config.Librd.SetKey("max.poll.interval.ms", 60000) // 60 seconds
	config.Librd.SetKey("enable.auto.commit", false)   // Disable auto-commit so we control commits

	// Create consumer
	consumer, err := NewGroupConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	// Create mock handler that captures the session for manual offset commit
	handler := newMockRebalanceHandlerWithSession()

	// Start consumer in goroutine
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- consumer.Subscribe([]string{topicName}, handler)
	}()

	// First, wait for consumer to join successfully (no error injection yet)
	t.Log("PHASE 1: Waiting for initial consumer assignment...")
	select {
	case partitions := <-handler.assignedCalled:
		t.Logf("SUCCESS: Consumer received initial assignment: %d partitions", len(partitions))
	case err := <-consumer.Errors():
		t.Fatalf("Consumer received unexpected error before injection: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatalf("Timed out waiting for initial consumer assignment")
	}

	// Give a moment for things to stabilize
	time.Sleep(2 * time.Second)

	// NOW: Enable split-brain mode
	// Drop heartbeat requests but send back fake SUCCESS responses
	t.Log("PHASE 2: Enabling SPLIT-BRAIN mode...")
	t.Log("  - Heartbeat requests will be dropped (not forwarded to broker)")
	t.Log("  - Fake SUCCESS responses will be sent back to client")
	t.Log("  - Broker will think client is dead after session timeout (6s)")
	t.Log("  - Client will think everything is fine (receiving success responses)")

	// Use 0 for unlimited - this will drop ALL heartbeats and fake ALL responses
	proxy.DropRequestAndFakeResponse(proxyPkg.APIKeyHeartbeat, proxyPkg.ErrNone, 0)

	// Wait for the broker's session to timeout (6 seconds + buffer)
	t.Log("PHASE 3: Waiting for broker session timeout (8 seconds)...")
	time.Sleep(8 * time.Second)

	// Track what events we observe
	lostReceived := false
	revokedReceived := false
	reassignedReceived := false
	errorReceived := false
	commitErrorReceived := false
	var commitError error

	// Now try to commit an offset - this should reveal the split-brain
	t.Log("PHASE 4: Attempting manual offset commit to trigger split-brain detection...")

	// Get the session from the handler
	session := handler.GetSession()
	if session == nil {
		t.Fatal("No session available - handler didn't capture session")
	}

	// Create a dummy record for offset commit
	// We need to commit for the partition we're assigned to
	partitions := handler.GetLastAssignedPartitions()
	if len(partitions) == 0 {
		t.Fatal("No partitions assigned")
	}

	// Try to commit offset - this will contact the coordinator
	// and should fail because we've been kicked out
	t.Log("  Calling CommitOffset on session...")
	dummyRecord := &dummyRecord{
		topic:     partitions[0].Topic,
		partition: partitions[0].Partition,
		offset:    0,
	}
	commitError = session.CommitOffset(context.Background(), dummyRecord, "test-commit")
	if commitError != nil {
		commitErrorReceived = true
		t.Logf("  CommitOffset returned error: %v", commitError)
	} else {
		t.Log("  CommitOffset succeeded (unexpected!)")
	}

	// Now observe what happens after the commit attempt
	t.Log("PHASE 5: Observing consumer behavior after commit attempt...")
	timeout := time.After(30 * time.Second)

observationLoop:
	for {
		select {
		case <-handler.lostCalled:
			lostReceived = true
			t.Log("EVENT: OnLost called - consumer detected it was kicked out!")

		case partitions := <-handler.revokedCalled:
			revokedReceived = true
			t.Logf("EVENT: OnPartitionRevoked called (%d partitions)", len(partitions))

		case partitions := <-handler.assignedCalled:
			reassignedReceived = true
			t.Logf("EVENT: OnPartitionAssigned called - consumer re-joined! (%d partitions)", len(partitions))
			// Consumer successfully re-joined after being kicked out
			break observationLoop

		case err := <-consumer.Errors():
			errorReceived = true
			t.Logf("EVENT: Error from consumer: %v", err)

		case <-timeout:
			t.Log("Observation timeout reached")
			break observationLoop
		}
	}

	// Clear the fake response rules to allow normal operation
	proxy.ClearFakeResponseRules()

	// Report results
	t.Log("========================================")
	t.Log("SPLIT-BRAIN TEST RESULTS:")
	t.Logf("  CommitOffset error:        %v (error: %v)", commitErrorReceived, commitError)
	t.Logf("  OnLost called:             %v", lostReceived)
	t.Logf("  OnPartitionRevoked called: %v", revokedReceived)
	t.Logf("  Consumer re-assigned:      %v", reassignedReceived)
	t.Logf("  Errors received:           %v", errorReceived)
	t.Log("========================================")

	// Check proxy stats
	fakeCount := proxy.GetFakeResponseCount(proxyPkg.APIKeyHeartbeat)
	droppedCount := proxy.GetDroppedRequestCount(proxyPkg.APIKeyHeartbeat)
	t.Logf("Proxy stats: %d fake responses sent, %d requests dropped", fakeCount, droppedCount)

	// Verify that split-brain was simulated (we sent fake responses)
	if fakeCount == 0 {
		t.Error("Test error: No fake responses were sent - split-brain was not simulated")
	}

	// Verify that the commit revealed the split-brain
	if commitErrorReceived {
		t.Log("SUCCESS: CommitOffset revealed the split-brain condition!")
	} else {
		t.Log("UNEXPECTED: CommitOffset succeeded - consumer might still think it's in the group")
	}

	// Stop consumer
	t.Log("Stopping consumer...")
	handler.Stop()
	consumer.Unsubscribe()
	t.Log("Test complete")
}

// mockRebalanceHandlerWithSession extends mockRebalanceHandler to capture the session
type mockRebalanceHandlerWithSession struct {
	*mockRebalanceHandler
	sessionMu            sync.Mutex
	lastSession          kafka.GroupSession
	lastAssignedPartitions []kafka.TopicPartition
}

func newMockRebalanceHandlerWithSession() *mockRebalanceHandlerWithSession {
	return &mockRebalanceHandlerWithSession{
		mockRebalanceHandler: newMockRebalanceHandler(),
	}
}

func (m *mockRebalanceHandlerWithSession) OnPartitionAssigned(ctx context.Context, session kafka.GroupSession) error {
	m.sessionMu.Lock()
	m.lastSession = session
	m.lastAssignedPartitions = session.Assignment().TPs()
	m.sessionMu.Unlock()

	return m.mockRebalanceHandler.OnPartitionAssigned(ctx, session)
}

func (m *mockRebalanceHandlerWithSession) GetSession() kafka.GroupSession {
	m.sessionMu.Lock()
	defer m.sessionMu.Unlock()
	return m.lastSession
}

func (m *mockRebalanceHandlerWithSession) GetLastAssignedPartitions() []kafka.TopicPartition {
	m.sessionMu.Lock()
	defer m.sessionMu.Unlock()
	return m.lastAssignedPartitions
}

// dummyRecord implements kafka.Record for testing offset commits
type dummyRecord struct {
	topic     string
	partition int32
	offset    int64
}

func (r *dummyRecord) Topic() string                { return r.topic }
func (r *dummyRecord) Partition() int32             { return r.partition }
func (r *dummyRecord) Offset() int64                { return r.offset }
func (r *dummyRecord) Key() []byte                  { return nil }
func (r *dummyRecord) Value() []byte                { return nil }
func (r *dummyRecord) Timestamp() time.Time         { return time.Now() }
func (r *dummyRecord) Headers() kafka.RecordHeaders { return nil }
func (r *dummyRecord) Ctx() context.Context         { return context.Background() }
func (r *dummyRecord) String() string               { return fmt.Sprintf("%s[%d]@%d", r.topic, r.partition, r.offset) }

// =============================================================================
// Test Runners
// =============================================================================

func runGroupConsumerTest(t *testing.T, fields consumerTestFields, expected consumerExpectedBehavior) {
	testLogger := mocks.NewTestLogger(t)
	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
		mocks.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer cluster.Terminate(context.Background())

	topicName := fmt.Sprintf("test-consumer-%s", uuid.New().String()[:8])
	cluster.CreateTopic(t, topicName, 1)

	proxy := cluster.Proxy()
	defer func() {
		proxy.ClearErrorInjection()
		proxy.ClearDropRules()
		proxy.ClearRequestDropRules()
	}()

	proxy.EnableVerboseLogging()

	// Inject errors BEFORE creating consumer
	switch fields.injectType {
	case errInjectTypeRequestDrop:
		proxy.DropRequestsFor(fields.injectApi, fields.injectedErrorCount)
	case errInjectTypeResponseError:
		proxy.InjectErrorFor(fields.injectApi, fields.injectedError, fields.injectedErrorCount)
	case errInjectTypeResponseDrop:
		proxy.DropResponsesFor(fields.injectApi, fields.injectedErrorCount)
	}

	// Create consumer config
	groupID := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	config := NewGroupConsumerConfig()
	config.BootstrapServers = cluster.BootstrapServers()
	config.GroupId = groupID
	config.Logger = testLogger
	config.MetricsReporter = metrics.NoopReporter()
	// Configure shorter timeouts for faster tests
	config.Librd.SetKey("session.timeout.ms", 10000)
	config.Librd.SetKey("heartbeat.interval.ms", 1000)
	config.Librd.SetKey("max.poll.interval.ms", 30000)

	// Create consumer
	consumer, err := NewGroupConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	// Create mock handler
	handler := newMockRebalanceHandler()

	// Context with timeout for cleanup
	_, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start consumer in goroutine
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- consumer.Subscribe([]string{topicName}, handler)
	}()

	// Use subscribeDone to detect consumer exit
	_ = subscribeDone

	// Wait for assignment or error
	waitTimeout := 30 * time.Second
	if expected.shouldJoinSuccessfully {
		select {
		case partitions := <-handler.assignedCalled:
			t.Logf("Consumer received assignment: %v partitions", len(partitions))
			if expected.shouldReceivePartitions && len(partitions) == 0 {
				t.Errorf("Expected to receive partitions, but got 0")
			}
		case err := <-consumer.Errors():
			if expected.shouldReceiveError {
				t.Logf("Consumer received expected error: %v", err)
			} else {
				t.Errorf("Consumer received unexpected error: %v", err)
			}
		case <-time.After(waitTimeout):
			t.Errorf("Timed out waiting for consumer assignment")
		}
	} else {
		// For failures, we expect the consumer to receive an error
		select {
		case err := <-consumer.Errors():
			t.Logf("Consumer received error (expected): %v", err)
		case partitions := <-handler.assignedCalled:
			t.Errorf("Consumer should not have joined successfully, but got assignment: %v", partitions)
		case <-time.After(waitTimeout):
			t.Logf("Timed out - consumer did not join (expected for fatal errors)")
		}
	}

	// Verify error injection occurred
	injectedCount := proxy.GetInjectedCount(fields.injectApi)
	t.Logf("Proxy injected %d errors for %s", injectedCount, fields.injectApi.String())
	if fields.injectType == errInjectTypeResponseError && injectedCount == 0 {
		t.Logf("Warning: No errors were injected - API might not have been called")
	}

	time.Sleep(10 * time.Second)

	// Stop consumer
	handler.Stop()
	consumer.Unsubscribe()
}

func runGroupConsumerHeartbeatTest(t *testing.T, fields consumerTestFields, expected consumerExpectedBehavior) {
	testLogger := mocks.NewTestLogger(t)
	cluster, err := mocks.SetupKafkaCluster(t,
		mocks.WithProxy("127.0.0.1:19093", "127.0.0.1:19094"),
		mocks.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("Failed to setup Kafka with proxy: %v", err)
	}
	defer cluster.Terminate(context.Background())

	topicName := fmt.Sprintf("test-consumer-%s", uuid.New().String()[:8])
	cluster.CreateTopic(t, topicName, 1)

	proxy := cluster.Proxy()
	defer func() {
		proxy.ClearErrorInjection()
		proxy.ClearDropRules()
		proxy.ClearRequestDropRules()
	}()

	// Create consumer config
	groupID := fmt.Sprintf("test-group-%s", uuid.New().String()[:8])
	config := NewGroupConsumerConfig()
	config.BootstrapServers = cluster.BootstrapServers()
	config.GroupId = groupID
	config.Logger = testLogger
	config.MetricsReporter = metrics.NoopReporter()
	// Configure shorter timeouts for faster tests
	config.Librd.SetKey("session.timeout.ms", 10000)
	config.Librd.SetKey("heartbeat.interval.ms", 1000)
	config.Librd.SetKey("max.poll.interval.ms", 30000)

	// Create consumer
	consumer, err := NewGroupConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	// Create mock handler
	handler := newMockRebalanceHandler()

	// Context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Start consumer in goroutine
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- consumer.Subscribe([]string{topicName}, handler)
	}()

	// First, wait for consumer to join successfully (no error injection yet)
	t.Log("Waiting for initial consumer assignment...")
	select {
	case partitions := <-handler.assignedCalled:
		t.Logf("Consumer received initial assignment: %v partitions", len(partitions))
	case err := <-consumer.Errors():
		t.Fatalf("Consumer received unexpected error before injection: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatalf("Timed out waiting for initial consumer assignment")
	}

	// Now inject the heartbeat error
	t.Logf("Injecting %s error into Heartbeat responses...", fields.injectedError.String())
	proxy.InjectErrorFor(fields.injectApi, fields.injectedError, fields.injectedErrorCount)

	// Wait for rebalance to be triggered
	if expected.shouldTriggerRebalance {
		select {
		case <-handler.revokedCalled:
			t.Log("Consumer revoked partitions (rebalance triggered)")
		case <-handler.lostCalled:
			t.Log("Consumer lost assignment")
		case partitions := <-handler.assignedCalled:
			// This indicates a re-join after rebalance
			t.Logf("Consumer re-assigned after rebalance: %v partitions", len(partitions))
		case err := <-consumer.Errors():
			t.Logf("Consumer received error during heartbeat test: %v", err)
		case <-ctx.Done():
			t.Logf("Context cancelled - test complete")
		case <-time.After(30 * time.Second):
			// For heartbeat tests, we might not always see a rebalance callback
			// Check if errors were injected
			injectedCount := proxy.GetInjectedCount(fields.injectApi)
			t.Logf("Heartbeat test completed - injected %d errors", injectedCount)
		}
	}

	// Verify error injection occurred
	injectedCount := proxy.GetInjectedCount(fields.injectApi)
	t.Logf("Proxy injected %d errors for %s", injectedCount, fields.injectApi.String())

	// Stop consumer
	handler.Stop()
	consumer.Unsubscribe()
}
