# GroupConsumer Error Handling Behavior

This document describes the error handling behavior of the librdkafka-based GroupConsumer, as verified through integration tests using a Kafka proxy for error injection.

## Table of Contents
- [Overview](#overview)
- [Test Infrastructure](#test-infrastructure)
- [Error Categories](#error-categories)
- [JoinGroup Error Handling](#joingroup-error-handling)
- [Heartbeat Error Handling](#heartbeat-error-handling)
- [SyncGroup Error Handling](#syncgroup-error-handling)
- [Split-Brain Scenario](#split-brain-scenario)
- [Key Findings](#key-findings)
- [Running the Tests](#running-the-tests)

## Overview

The GroupConsumer uses librdkafka under the hood for Kafka consumer group coordination. Understanding how it handles various error scenarios is critical for building resilient streaming applications.

### Consumer Group Protocol Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Consumer Group Join Flow                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   Consumer                    Coordinator (Broker)                   │
│      │                              │                                │
│      │──── FindCoordinator ────────>│                                │
│      │<─── Coordinator Info ────────│                                │
│      │                              │                                │
│      │──── JoinGroup ──────────────>│  ← Error injection point      │
│      │<─── JoinGroup Response ──────│                                │
│      │                              │                                │
│      │──── SyncGroup ──────────────>│  ← Error injection point      │
│      │<─── Assignment ──────────────│                                │
│      │                              │                                │
│      │──── Heartbeat (periodic) ───>│  ← Error injection point      │
│      │<─── Heartbeat Response ──────│                                │
│      │                              │                                │
└─────────────────────────────────────────────────────────────────────┘
```

## Test Infrastructure

### Proxy-Based Error Injection

Tests use a TCP proxy that sits between the consumer and Kafka broker. The proxy can:
- **Inject error codes** into responses (modifies the Kafka protocol error field)
- **Drop requests** (simulates network issues)
- **Drop responses** (simulates partial network failures)
- **Fake responses** (drop request but send fake response to client - for split-brain testing)

```go
// Example: Inject COORDINATOR_NOT_AVAILABLE into next 2 JoinGroup responses
proxy.InjectErrorFor(proxyPkg.APIKeyJoinGroup, proxyPkg.ErrCoordinatorNotAvailable, 2)

// Example: Drop heartbeat requests but send fake SUCCESS to client (split-brain)
proxy.DropRequestAndFakeResponse(proxyPkg.APIKeyHeartbeat, proxyPkg.ErrNone, 0)
```

### Mock RebalanceHandler

Tests use a mock `RebalanceHandler` that tracks callback invocations via channels:

```go
type mockRebalanceHandler struct {
    assignedCalled chan []kafka.TopicPartition  // Signals OnPartitionAssigned
    revokedCalled  chan []kafka.TopicPartition  // Signals OnPartitionRevoked
    lostCalled     chan struct{}                 // Signals OnLost
    consumeCalled  chan kafka.TopicPartition    // Signals Consume started
}
```

## Error Categories

### Retriable Errors
These errors cause librdkafka to automatically retry the operation:

| Error Code | Name | Description |
|------------|------|-------------|
| 15 | COORDINATOR_NOT_AVAILABLE | Group coordinator is not available |
| 16 | NOT_COORDINATOR | Broker is not the coordinator for this group |
| 27 | REBALANCE_IN_PROGRESS | Group is rebalancing |
| 14 | COORDINATOR_LOAD_IN_PROGRESS | Coordinator is loading |

### Fatal Errors
These errors prevent successful group join when continuously injected:

| Error Code | Name | Description |
|------------|------|-------------|
| 30 | GROUP_AUTHORIZATION_FAILED | Not authorized to access group |
| 24 | INVALID_GROUP_ID | Invalid group ID |

### Re-join Trigger Errors
These errors cause the consumer to leave and re-join the group:

| Error Code | Name | Description |
|------------|------|-------------|
| 22 | ILLEGAL_GENERATION | Generation ID is stale |
| 25 | UNKNOWN_MEMBER_ID | Member ID is not recognized |
| 79 | MEMBER_ID_REQUIRED | Must provide member ID (KIP-394) |

## JoinGroup Error Handling

### Test Results

| Scenario | Injected Error | Error Count | Result |
|----------|---------------|-------------|--------|
| Coordinator unavailable | COORDINATOR_NOT_AVAILABLE (15) | 2 | Retries and joins successfully |
| Wrong coordinator | NOT_COORDINATOR (16) | 2 | Retries and joins successfully |
| Rebalance in progress | REBALANCE_IN_PROGRESS (27) | 2 | Retries and joins successfully |
| Authorization failed | GROUP_AUTHORIZATION_FAILED (30) | 100 | Never joins, errors reported |
| Invalid group | INVALID_GROUP_ID (24) | 100 | Never joins, errors reported |
| Unknown member | UNKNOWN_MEMBER_ID (25) | 1 | Re-joins with new member ID |

### Behavior Details

#### Retriable Errors (15, 16, 27)
```
Consumer                    Proxy                      Broker
    │                         │                          │
    │─── JoinGroup ──────────>│                          │
    │                         │─── JoinGroup ───────────>│
    │                         │<── Response (OK) ────────│
    │<── ERROR INJECTED ──────│  (proxy modifies error)  │
    │                         │                          │
    │    [librdkafka retries after backoff]              │
    │                         │                          │
    │─── JoinGroup ──────────>│                          │
    │                         │─── JoinGroup ───────────>│
    │                         │<── Response (OK) ────────│
    │<── Response (OK) ───────│  (no more injections)    │
    │                         │                          │
    │    [OnPartitionAssigned callback invoked]          │
```

#### Fatal Errors (24, 30)
When continuously injected, the consumer keeps retrying but never succeeds:
- Errors are reported via the `Errors()` channel
- `OnPartitionAssigned` is never called
- Consumer remains in a retry loop

**Important**: librdkafka does NOT treat these as immediately fatal - it will keep retrying. The errors are surfaced to the application via the error channel.

## Heartbeat Error Handling

Heartbeat errors occur AFTER the consumer has successfully joined and is maintaining its session.

### Test Results

| Scenario | Injected Error | Result |
|----------|---------------|--------|
| Rebalance needed | REBALANCE_IN_PROGRESS (27) | Triggers rebalance, revokes partitions |
| Stale generation | ILLEGAL_GENERATION (22) | Triggers OnLost, re-joins group |
| Unknown member | UNKNOWN_MEMBER_ID (25) | Triggers OnLost, re-joins group |

### Behavior Details

#### Heartbeat Triggers Rebalance
```
Consumer                    Proxy                      Broker
    │                         │                          │
    │  [Consumer has joined, consuming messages]         │
    │                         │                          │
    │─── Heartbeat ──────────>│                          │
    │                         │─── Heartbeat ───────────>│
    │                         │<── Response (OK) ────────│
    │<── REBALANCE_IN_PROGRESS│  (error injected)        │
    │                         │                          │
    │    [OnPartitionRevoked callback]                   │
    │    [Re-joins group]                                │
    │    [OnPartitionAssigned callback]                  │
```

#### Heartbeat Triggers OnLost
For `ILLEGAL_GENERATION` or `UNKNOWN_MEMBER_ID`:
```
Consumer                    Proxy                      Broker
    │                         │                          │
    │─── Heartbeat ──────────>│                          │
    │                         │─── Heartbeat ───────────>│
    │                         │<── Response (OK) ────────│
    │<── UNKNOWN_MEMBER_ID ───│  (error injected)        │
    │                         │                          │
    │    [OnLost callback - assignment lost]             │
    │    [OnPartitionRevoked callback]                   │
    │    [Re-joins group automatically]                  │
```

## SyncGroup Error Handling

SyncGroup errors occur between JoinGroup success and partition assignment.

| Error Code | Expected Behavior |
|------------|-------------------|
| COORDINATOR_NOT_AVAILABLE (15) | Retry |
| NOT_COORDINATOR (16) | Retry |
| REBALANCE_IN_PROGRESS (27) | Re-join group |
| ILLEGAL_GENERATION (22) | Re-join group |
| UNKNOWN_MEMBER_ID (25) | Re-join group |
| GROUP_AUTHORIZATION_FAILED (30) | Fatal error |

## Split-Brain Scenario

A **split-brain** scenario occurs when a consumer believes it's connected and healthy, but the broker has already removed it from the group. This can happen when:
- Network issues prevent heartbeats from reaching the broker
- A proxy or load balancer responds with cached/fake responses
- Partial network failures allow some traffic but not heartbeats

### The Danger: Zombie Consumers

In a split-brain scenario, the consumer becomes a **"zombie"** - it thinks it's alive and part of the group, but:
- The broker has timed out the consumer's session
- Another consumer may have taken over its partitions
- The zombie consumer continues processing, potentially causing **duplicate message processing**

### Test Setup: Fake Heartbeat Responses

The proxy can simulate split-brain by:
1. Intercepting heartbeat requests from the consumer
2. Sending back **fake SUCCESS responses** to the consumer
3. **NOT forwarding** the heartbeat to the broker

```go
// Enable split-brain mode: drop heartbeats but fake success responses
proxy.DropRequestAndFakeResponse(proxyPkg.APIKeyHeartbeat, proxyPkg.ErrNone, 0)
```

### Split-Brain Flow Diagram

```
Consumer                    Proxy                      Broker
    │                         │                          │
    │  [Consumer is active, consuming messages]          │
    │                         │                          │
    │─── Heartbeat ──────────>│                          │
    │                         │   (REQUEST DROPPED)      │
    │<── FAKE SUCCESS ────────│                          │
    │                         │                          │
    │  [Consumer thinks it's alive]                      │
    │                         │      [No heartbeat       │
    │─── Heartbeat ──────────>│       received!]         │
    │                         │   (REQUEST DROPPED)      │
    │<── FAKE SUCCESS ────────│                          │
    │                         │                          │
    │  [Consumer still happy]     [Session timeout!      │
    │                              Consumer removed      │
    │─── Heartbeat ──────────>│    from group]           │
    │                         │   (REQUEST DROPPED)      │
    │<── FAKE SUCCESS ────────│                          │
    │                         │                          │
    │  [ZOMBIE STATE: Consumer thinks it's in group]     │
    │  [But broker has kicked it out]                    │
    │                         │                          │
    │─── OffsetCommit ───────>│                          │
    │                         │─── OffsetCommit ────────>│
    │                         │<── UNKNOWN_MEMBER_ID ────│
    │<── UNKNOWN_MEMBER_ID ───│                          │
    │                         │                          │
    │  [SPLIT-BRAIN DETECTED!]                           │
    │  [OnLost callback]                                 │
    │  [OnPartitionRevoked callback]                     │
    │  [Re-joins group]                                  │
```

### Test Results

| Phase | Action | Result |
|-------|--------|--------|
| 1. Join | Consumer joins group normally | OnPartitionAssigned called |
| 2. Split-Brain | Enable fake heartbeat responses | Consumer receives SUCCESS, broker receives nothing |
| 3. Session Timeout | Wait for broker timeout (6s) | Broker removes consumer from group |
| 4. Zombie State | Consumer continues operating | Consumer believes it's alive |
| 5. Detection | Manual offset commit attempt | `UNKNOWN_MEMBER_ID` error returned |
| 6. Recovery | Consumer detects loss | OnLost → OnPartitionRevoked → Re-join → OnPartitionAssigned |

### Key Observation: Detection Requires Coordinator Interaction

**Critical Finding**: Without explicit coordinator interaction, a consumer can remain in zombie state **indefinitely**.

The consumer only discovers it was kicked out when it:
1. **Commits offsets** (OffsetCommit request)
2. **Another consumer triggers rebalance** (JoinGroup from another member)
3. **Fetches from a partition it no longer owns** (Fetch returns error)

If the consumer is only fetching (and auto-commit is disabled), it may never detect the split-brain!

### Mitigation Strategies

1. **Regular Offset Commits**: Even with manual commit, periodically commit offsets to detect split-brain
   ```go
   // Commit periodically to detect if consumer was kicked out
   ticker := time.NewTicker(30 * time.Second)
   for range ticker.C {
       session.CommitOffset(ctx, lastRecord, "periodic-commit")
   }
   ```

2. **Monitor Error Channel**: Always monitor the `Errors()` channel for unexpected errors

3. **Use Cooperative Rebalancing**: Cooperative rebalancing may provide earlier detection

4. **Implement Health Checks**: Application-level health checks that verify group membership

### Running the Split-Brain Test

```bash
go test -v -run "TestGroupConsumer_SplitBrain" ./kafka/adaptors/librd/... -timeout 180s
```

## Key Findings

### 1. librdkafka Retries Most Errors
Unlike what might be expected, librdkafka does not immediately fail on "fatal" errors like `GROUP_AUTHORIZATION_FAILED`. Instead, it:
- Reports the error via the error callback/channel
- Waits with backoff
- Retries the operation

This means applications should:
- Monitor the `Errors()` channel for repeated errors
- Implement their own logic to decide when to give up

### 2. Error Channel is Essential
The `Errors()` channel is the primary way to detect problems:
```go
go func() {
    for err := range consumer.Errors() {
        log.Printf("Consumer error: %v", err)
        // Implement your error handling logic
    }
}()
```

### 3. Rebalance Callbacks Indicate State Changes
- `OnPartitionAssigned`: Consumer successfully joined and received partitions
- `OnPartitionRevoked`: Partitions being revoked (cooperative or eager)
- `OnLost`: Assignment was lost unexpectedly (session timeout, unknown member, etc.)

### 4. Session Timeout Behavior
When heartbeats fail repeatedly or responses are dropped:
- Consumer's session eventually times out on the broker
- `OnLost` is called
- Consumer automatically attempts to re-join

### 5. Error Code Translation
The proxy modifies the original error code in responses. The original error codes observed:
- `16` (NOT_COORDINATOR) - Normal response during coordinator discovery
- `79` (MEMBER_ID_REQUIRED) - KIP-394 behavior, consumer retries with member ID
- `0` (NO_ERROR) - Normal successful response

## Running the Tests

### Run All Consumer Tests
```bash
go test -v -run "TestGroupConsumer_" ./kafka/adaptors/librd/... -timeout 600s
```

### Run Specific Test
```bash
# JoinGroup tests only
go test -v -run "TestGroupConsumer_JoinGroup" ./kafka/adaptors/librd/... -timeout 300s

# Heartbeat tests only
go test -v -run "TestGroupConsumer_Heartbeat" ./kafka/adaptors/librd/... -timeout 300s

# Single test case
go test -v -run "TestGroupConsumer_JoinGroup/CoordinatorNotAvailable" ./kafka/adaptors/librd/... -timeout 120s
```

### Test Output Interpretation
```
proxy.go:640: proxy: INJECTING ERROR 15 (COORDINATOR_NOT_AVAILABLE) into JoinGroup response (was 16, correlation_id=3)
                     ↑                    ↑                              ↑
                     Injected error       API being modified             Original error in response
```

## Appendix: Kafka Error Codes Reference

| Code | Name | Retriable | Description |
|------|------|-----------|-------------|
| 14 | COORDINATOR_LOAD_IN_PROGRESS | Yes | Coordinator is loading |
| 15 | COORDINATOR_NOT_AVAILABLE | Yes | Coordinator not available |
| 16 | NOT_COORDINATOR | Yes | Not the coordinator |
| 22 | ILLEGAL_GENERATION | No* | Stale generation ID |
| 23 | INCONSISTENT_GROUP_PROTOCOL | No | Protocol mismatch |
| 24 | INVALID_GROUP_ID | No | Invalid group ID |
| 25 | UNKNOWN_MEMBER_ID | No* | Unknown member |
| 27 | REBALANCE_IN_PROGRESS | Yes | Rebalance in progress |
| 30 | GROUP_AUTHORIZATION_FAILED | No | Authorization failed |
| 79 | MEMBER_ID_REQUIRED | No* | Must provide member ID |
| 82 | FENCED_INSTANCE_ID | No | Static member fenced |

*These errors trigger a re-join rather than a simple retry.