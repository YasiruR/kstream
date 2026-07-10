# librd Kafka Adaptor

This package provides Kafka producer and consumer adaptors using the [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) library (librdkafka).

## Transaction Error Handling Research

This document details the findings from extensive research into Kafka transactional producer behavior, specifically focusing on the "in-doubt" transaction scenario.

### Research Objective

Investigate and test the behavior when a Kafka transactional commit succeeds on the broker but the acknowledgment (EndTxn response) is lost before reaching the producer. This is known as an "in-doubt" transaction scenario.

### What the Documentation Says

According to [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) and official Kafka documentation:

#### Transaction Commit is Multi-Phase

When a producer calls `CommitTransaction()`, Kafka executes a two-phase commit:

1. **Phase 1: Prepare**
   - Transaction coordinator writes `PREPARE_COMMIT` to the transaction log (`__transaction_state` topic)
   - This message is replicated for durability
   - Once replicated, the transaction is **guaranteed to complete** regardless of subsequent failures

2. **Phase 2: Write Commit Markers**
   - Coordinator sends `WriteTxnMarkerRequest` to every partition leader that participated in the transaction
   - Each partition leader appends a `COMMIT` control record to its log
   - These control records mark transaction boundaries for consumers using `isolation.level=read_committed`

3. **Phase 3: Finalize**
   - Once all WriteTxnMarker requests complete, coordinator writes `COMMITTED` to the transaction log
   - **THEN** the EndTxnResponse is sent to the producer

#### Expected Behavior for Lost ACK

Based on the documentation:
- The broker should complete the commit **before** sending the response
- If the response is lost, the transaction should already be committed
- A retry of EndTxn should be idempotent - the coordinator recognizes the transaction is already complete
- The transaction log in `__transaction_state` is the authoritative source of truth

### Testing Approach

We built a custom Kafka protocol-aware TCP proxy (`kafka_proxy_test.go`) to test various failure scenarios:

#### Custom proxy Features

```go
type KafkaProtocolProxy struct {
    dropAPIKeys  map[KafkaAPIKey]bool          // Drop ALL responses for an API key
    dropFirstN   map[KafkaAPIKey]int           // Drop first N responses only
    delayAPIKeys map[KafkaAPIKey]time.Duration // Delay responses
    closeOnDrop  bool                          // Close connection after dropping (prevents librdkafka crash)
}
```

The proxy:
- Parses Kafka protocol at the TCP level
- Tracks request/response correlation IDs
- Can selectively drop or delay EndTxn (API Key 26) responses
- Lets all other traffic through normally
- Supports `closeOnDrop` mode to prevent librdkafka state machine crashes

#### Test Setup

1. Kafka container configured to advertise the proxy address
2. Producer connects through proxy
3. All traffic routes through proxy
4. Consumers connect directly to Kafka (bypassing proxy) for verification

### Test Results

> **Important Note on Test Environment:** The Kafka container must be configured with separate INTERNAL and EXTERNAL listeners. The INTERNAL listener is used by the TransactionCoordinator for inter-broker communication inside the container. Without this, the TransactionCoordinator cannot connect to itself, causing transactions to silently fail regardless of proxy behavior.

#### Test 1: Drop ALL EndTxn Responses (with closeOnDrop)

**Configuration:**
- `proxy.DropResponsesFor(APIKeyEndTxn)` - Drop all EndTxn responses indefinitely
- `proxy.EnableCloseOnDrop()` - Close connection after dropping to prevent librdkafka crash
- `transaction.timeout.ms = 60000` (60 seconds)
- Context timeout: 15 seconds

**Results:**
```
proxy: Intercepted EndTxn request (correlation_id=17)
proxy: DROPPING EndTxn response (correlation_id=17)
proxy: Closing connection after dropping response
proxy: Intercepted EndTxn request (correlation_id=19)
proxy: DROPPING EndTxn response (correlation_id=19)
proxy: Closing connection after dropping response
...
CommitTransaction returned after 15.00s
proxy dropped 8 EndTxn response(s)
Message found with read_uncommitted: YES
Message found with read_committed: YES   <-- TRUE IN-DOUBT SCENARIO!
```

**Observation:** Transaction WAS committed on the broker even though the producer received a timeout error. This is a true in-doubt scenario.

---

#### Test 2: Delay EndTxn Response (without closeOnDrop)

**Configuration:**
- `proxy.DelayResponsesFor(APIKeyEndTxn, 20*time.Second)`
- Context timeout: 10 seconds

**Results:**
```
proxy: Intercepted EndTxn request (correlation_id=17)
proxy: DELAYING EndTxn response by 20s (correlation_id=17)
CommitTransaction returned after 10.00s (context timeout)
... 15 seconds later ...
Assertion failed: (!*"BUG: Invalid transaction state transition")
signal: abort trap
```

**Observation:** librdkafka crashed when the delayed response arrived after it had already timed out internally. This is why `closeOnDrop` is necessary.

---

### Key Findings

#### 1. True "In-Doubt" Scenario IS Achievable

When the EndTxn response is dropped (but the request reaches the broker):
- The broker **DOES complete the commit** before/during sending the response
- The message IS visible with `read_committed`
- The producer receives a timeout error and thinks the commit failed
- This is the exact "in-doubt" transaction scenario described in KIP-98

**This confirms the Kafka documentation is correct:** the broker completes the commit before the response is sent to the producer.

#### 2. librdkafka DOES Retry EndTxn Internally

When the connection is closed after dropping a response:
- librdkafka detects the disconnection
- It reconnects using exponential backoff (`reconnect.backoff.ms`, `reconnect.backoff.max.ms`)
- It retries the EndTxn request on the new connection
- This continues until the context timeout is reached

**Note:** The retry behavior is controlled by librdkafka's reconnection settings, NOT `transaction.timeout.ms`:
- `reconnect.backoff.ms` (default: 100ms) - Initial backoff before reconnecting
- `reconnect.backoff.max.ms` (default: 10000ms) - Maximum backoff (exponential growth)

#### 3. Delayed/Late Responses Crash librdkafka

When an EndTxn response arrives after librdkafka has already timed out:
- librdkafka crashes with "Invalid transaction state transition"
- This is a bug in librdkafka's state machine handling
- **Workaround:** Use `closeOnDrop` mode to close the connection immediately after dropping, forcing a clean reconnection

#### 4. Container Networking is Critical for Testing

Initial tests showed transactions not committing, but this was due to misconfigured Kafka container networking:
- The TransactionCoordinator needs to connect to itself inside the container
- If Kafka only has an EXTERNAL listener advertising the proxy address, the coordinator cannot connect
- **Solution:** Configure separate INTERNAL (for inter-broker) and EXTERNAL (for clients) listeners

```go
// Correct Kafka container configuration
"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
"KAFKA_LISTENERS":                      "INTERNAL://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093",
"KAFKA_ADVERTISED_LISTENERS":           "INTERNAL://localhost:9093,EXTERNAL://127.0.0.1:" + proxyPort,
"KAFKA_INTER_BROKER_LISTENER_NAME":     "INTERNAL",
```

### Error Classification Behavior

The `handleTxError` function in `transactional_producer.go` correctly handles these scenarios:

| Scenario | Error Returned | Classification |
|----------|---------------|----------------|
| EndTxn response dropped | Timeout | `ShouldShutdown=true` |
| EndTxn response delayed | Timeout | `ShouldShutdown=true` |
| Multiple retries exhausted | Max retry exceeded | `ShouldShutdown=true` |
| Producer fenced | Fencing error | `RequiresRestart=true` |
| Transaction state error | State error | `TxnRequiresAbort=true` |

### Handling In-Doubt Transactions: Safety Mechanisms

#### 1. `INVALID_TXN_STATE` as a Safety Signal

When a producer receives `TxnRequiresAbort=true` and attempts to abort, but the transaction was actually already committed on the broker, the abort will fail with:

```
Fatal error: Failed to end transaction: Broker: Producer attempted a transactional operation in an invalid state
```

This `INVALID_TXN_STATE` error is actually a **safety mechanism**:

| Scenario | Abort Result | Meaning | Action |
|----------|--------------|---------|--------|
| Commit actually failed | Abort succeeds | Transaction rolled back | Safe to retry business logic |
| Commit actually succeeded | `INVALID_TXN_STATE` | Transaction already complete | **Don't retry** - already committed |

**This prevents duplicate processing:** The application can detect that the transaction was actually committed and avoid re-processing.

```go
err := producer.CommitTransaction(ctx)
if err != nil {
    producerErr, ok := err.(Err)
    if ok && producerErr.TxnRequiresAbort() {
        abortErr := producer.AbortTransaction(ctx)
        if abortErr != nil && strings.Contains(abortErr.Error(), "invalid state") {
            // Transaction was actually committed!
            log.Info("Transaction was committed - no retry needed")
            return nil  // Success - no duplicate processing
        }
        // Abort succeeded - transaction really failed, safe to retry
        return retryBusinessLogic()
    }
}
```

**Trade-off:** `INVALID_TXN_STATE` is classified as a fatal error requiring producer restart, but this is much better than silent duplicate processing.

#### 2. Critical Startup Order: `InitTransactions()` Before Consumer

When an application restarts after a crash during commit, there's a race condition if the consumer starts before `InitTransactions()` completes:

```
WRONG ORDER (Race Condition):
─────────────────────────────────────────────────────────────────────
Old Producer:  Produce → SendOffsets(100) → EndTxn(COMMIT) → 💥 CRASH
                                                    ↓
                                           [Broker still processing...]
─────────────────────────────────────────────────────────────────────
                        APPLICATION RESTARTS
─────────────────────────────────────────────────────────────────────
Consumer starts → FetchOffset → Gets offset 50 (tx still pending)
                         ↓
                    [Broker commits old tx] ← Offset is now 100!
                         ↓
Producer → InitTransactions() → Tx already committed, nothing to do
                         ↓
Consumer processing from 50... but committed offset is 100
                         ↓
                    DUPLICATES! (messages 50-99 processed twice)
```

**Correct startup order:**

```go
// On application startup/restart

// 1. FIRST: Initialize producer and resolve any pending transactions
producer := NewProducer(config)  // Same transactional.id
err := producer.InitTransactions(ctx)  // ← MUST complete first!
if err != nil {
    return err
}

// 2. THEN: Start consumer (it will now see stable offsets)
consumer := NewConsumer(config)
consumer.Subscribe(topics)

// 3. Now safe to process
for {
    messages := consumer.Poll()
    // ...
}
```

**Why this works:**
- `InitTransactions()` resolves any pending transactions (commits or aborts them)
- Only after resolution is the consumer offset in a stable state
- Consumer then reads from the correct, stable offset
- No race condition, no duplicates

#### 3. Same `transactional.id` is Required

**Critical:** The entire recovery mechanism depends on the new producer using the **same `transactional.id`** as the crashed producer.

```
transactional.id = "order-processor-1"
         ↓
┌─────────────────────────────────────────────────────────┐
│  Transaction Coordinator (__transaction_state topic)    │
│                                                         │
│  transactional.id: "order-processor-1"                  │
│  producer_id: 1000                                      │
│  epoch: 5                                               │
│  state: ONGOING  ← pending transaction from crash       │
└─────────────────────────────────────────────────────────┘

New producer with SAME transactional.id:
  InitTransactions("order-processor-1")
         ↓
  Coordinator:
    - Bumps epoch: 5 → 6 (fences old producer)
    - Aborts pending transaction (ONGOING → ABORTED)
    - Consumer offsets rolled back
    - Returns new producer_id + epoch
         ↓
  Recovery complete ✓
```

If the new producer uses a **different** `transactional.id`:

```
New producer with DIFFERENT transactional.id:
  InitTransactions("order-processor-2")  // Different ID!
         ↓
  Coordinator creates NEW entry for "order-processor-2"
         ↓
  OLD transaction ("order-processor-1") still PENDING!
    - Remains ONGOING until transaction.timeout.ms expires
    - Consumer offsets in limbo
    - Can cause duplicates or data loss!
```

**Best Practice:** Derive `transactional.id` from a stable identifier:
```go
// Good: Stable ID based on partition assignment
transactionalId := fmt.Sprintf("processor-%s-partition-%d", appName, partition)

// Bad: Random or timestamp-based ID
transactionalId := fmt.Sprintf("processor-%d", time.Now().UnixNano())  // ❌
```

### Implications for Production Systems

1. **Timeout Errors Require Producer Restart**
   - When `ShouldShutdown=true`, the producer must be recreated
   - The same transactional ID can be reused
   - `InitTransactions()` will fence the old producer and abort any pending transactions

2. **Network Issues CAN Result in In-Doubt Transactions**
   - When EndTxn responses are lost, transactions MAY have been committed
   - The producer cannot know whether the commit succeeded or failed
   - This is why conservative error handling (`ShouldShutdown=true`) is critical

3. **Conservative Error Handling is Essential**
   - Treating timeouts as fatal errors requiring shutdown is the safest approach
   - The transaction might have actually succeeded on the broker
   - Creating a new producer with `InitTransactions()` will:
     - Fence the old producer (increment epoch)
     - Abort any truly uncommitted transactions
     - Leave already-committed transactions intact (idempotent)

4. **Downstream Handling of In-Doubt Scenarios**
   - Applications should be designed to handle duplicate processing
   - Use idempotent consumers or deduplication logic where possible
   - The exactly-once guarantee holds as long as the producer correctly handles errors

### Test Files

- `kafka_proxy_test.go` - Custom Kafka protocol-aware TCP proxy
- `transactional_producer_integration_test.go` - Integration tests including:
  - `TestIntegration_InDoubt_CustomProxy` - Tests dropping EndTxn responses
  - `TestIntegration_InDoubt_DelayedResponse` - Tests delaying EndTxn responses
  - `TestIntegration_DropFirstResponse_RetrySucceeds` - Tests dropping only first response
- `testutil_integration.go` - Test utilities including `setupKafkaWithCustomProxy()`

### Running the Tests

```bash
# Run all integration tests
go test -tags=integration -v ./kafka/adaptors/librd/... -timeout 300s

# Run specific in-doubt scenario tests
go test -tags=integration -v ./kafka/adaptors/librd/... -run "TestIntegration_InDoubt" -timeout 180s

# Run drop-first-response test
go test -tags=integration -v ./kafka/adaptors/librd/... -run "TestIntegration_DropFirstResponse" -timeout 120s
```

### References

- [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
- [KIP-890: Transactions Server-Side Defense](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense)
- [KAFKA-17754: A delayed EndTxn message can cause aborted read, lost writes, atomicity violation](https://issues.apache.org/jira/browse/KAFKA-17754)
- [Kafka Transaction Protocol Documentation](https://kafka.apache.org/documentation/#transaction_protocol)
- [librdkafka Transactional Producer](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#transactions)
- [librdkafka Configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) - Includes `reconnect.backoff.ms` and `reconnect.backoff.max.ms` settings
