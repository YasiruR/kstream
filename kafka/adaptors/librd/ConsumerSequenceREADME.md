# GroupConsumer Protocol Sequence Documentation

This document details the exact sequence of Kafka protocol messages during consumer group operations, captured from integration tests with microsecond-precision timestamps.

## Table of Contents
- [Consumer Join Sequence](#consumer-join-sequence)
- [Error Recovery Sequence](#error-recovery-sequence)
- [Heartbeat Maintenance](#heartbeat-maintenance)
- [Consumer Shutdown Sequence](#consumer-shutdown-sequence)
- [KIP-394: Member ID Required](#kip-394-member-id-required)
- [Timing Analysis](#timing-analysis)
- [State Store Recovery Pattern](#state-store-recovery-pattern)

## Consumer Join Sequence

### Normal Flow (No Errors)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                       Consumer Group Join Sequence                            │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  Consumer                        Proxy                         Broker         │
│     │                              │                              │           │
│     │─────── ApiVersions ─────────>│──────────────────────────────>│          │
│     │<────── ApiVersions ──────────│<──────────────────────────────│          │
│     │                              │                              │           │
│     │─────── Metadata ────────────>│──────────────────────────────>│          │
│     │<────── Metadata ─────────────│<──────────────────────────────│          │
│     │                              │                              │           │
│     │─────── FindCoordinator ─────>│──────────────────────────────>│          │
│     │<────── Coordinator Info ─────│<──────────────────────────────│          │
│     │                              │                              │           │
│     │─────── JoinGroup (empty) ───>│──────────────────────────────>│          │
│     │<────── MEMBER_ID_REQUIRED ───│<──────────────────────────────│  KIP-394 │
│     │                              │                              │           │
│     │─────── JoinGroup (w/ID) ────>│──────────────────────────────>│          │
│     │<────── Leader Assignment ────│<──────────────────────────────│          │
│     │                              │                              │           │
│     │─────── SyncGroup ───────────>│──────────────────────────────>│          │
│     │<────── Partition Assignment ─│<──────────────────────────────│          │
│     │                              │                              │           │
│     │  [OnPartitionAssigned callback]                             │           │
│     │                              │                              │           │
│     │─────── Heartbeat ───────────>│──────────────────────────────>│          │
│     │<────── OK ───────────────────│<──────────────────────────────│          │
│     │        (every ~1 second)     │                              │           │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Captured Sequence (From Test Run)

| Timestamp | API | Direction | Details |
|-----------|-----|-----------|---------|
| 18:37:26.624429 | - | - | Consumer Subscribe() called |
| 18:37:26.624964 | ApiVersions | REQUEST | Client initialization |
| 18:37:26.627726 | ApiVersions | RESPONSE | 429 bytes, latency=2.76ms |
| 18:37:26.627904 | Metadata | REQUEST | Get cluster metadata |
| 18:37:26.627928 | FindCoordinator | REQUEST | Find group coordinator |
| 18:37:26.629163 | Metadata | RESPONSE | Cluster info received |
| 18:37:26.637018 | FindCoordinator | RESPONSE | Coordinator=127.0.0.1:19094 |

## Error Recovery Sequence

When `COORDINATOR_NOT_AVAILABLE` error is injected, librdkafka automatically retries with exponential backoff.

### Sequence with 2 Injected Errors

```
Time (ms)     Event
─────────────────────────────────────────────────────────────────────────
0             Subscribe() called
1010          JoinGroup #1 → COORDINATOR_NOT_AVAILABLE (injected, was 16)
1013          FindCoordinator (retry)
              [~3 second backoff]
4008          JoinGroup #2 → COORDINATOR_NOT_AVAILABLE (injected, was 79)
4035          FindCoordinator (retry)
              [~2 second backoff]
6014          JoinGroup #3 → MEMBER_ID_REQUIRED (79) - normal KIP-394
6017          JoinGroup #4 (with member ID) → SUCCESS
6043          Metadata request
6045          SyncGroup → Partition assignment received
6073          OnPartitionAssigned callback triggered
6073          Heartbeat #1 sent
```

### Captured Error Recovery (From Test Run)

| Timestamp | Event | Original Error | Injected Error |
|-----------|-------|----------------|----------------|
| 18:37:27.636745 | JoinGroup #1 REQUEST | - | - |
| 18:37:27.639843 | JoinGroup #1 RESPONSE | 16 (NOT_COORDINATOR) | 15 (COORDINATOR_NOT_AVAILABLE) |
| 18:37:27.639973 | FindCoordinator | Retry after error | - |
| 18:37:30.634100 | JoinGroup #2 REQUEST | - | - |
| 18:37:30.660791 | JoinGroup #2 RESPONSE | 79 (MEMBER_ID_REQUIRED) | 15 (COORDINATOR_NOT_AVAILABLE) |
| 18:37:30.661815 | FindCoordinator | Retry after error | - |
| 18:37:32.640529 | JoinGroup #3 REQUEST | - | No injection |
| 18:37:32.643549 | JoinGroup #3 RESPONSE | 79 (MEMBER_ID_REQUIRED) | - |
| 18:37:32.643805 | JoinGroup #4 REQUEST | With member ID | - |
| 18:37:32.669109 | JoinGroup #4 RESPONSE | SUCCESS (leader) | - |

### Retry Backoff Timing

```
JoinGroup #1 failed:  18:37:27.639843
JoinGroup #2 retry:   18:37:30.634100  (backoff: ~3 seconds)
JoinGroup #3 retry:   18:37:32.640529  (backoff: ~2 seconds)
```

## KIP-394: Member ID Required

Starting with Kafka 2.2+ (KIP-394), the JoinGroup protocol requires two phases:

### Phase 1: Get Member ID
```
Consumer → JoinGroup (member_id="")
Broker   → Response (error=MEMBER_ID_REQUIRED, member_id="consumer-xxx-uuid")
```

### Phase 2: Join with Member ID
```
Consumer → JoinGroup (member_id="consumer-xxx-uuid")
Broker   → Response (error=NONE, leader assignment)
```

### Why This Matters

In the test output, you'll see:
- **Error code 79** = `MEMBER_ID_REQUIRED`
- This is NOT an error - it's normal protocol behavior
- The consumer receives a member ID and immediately retries

```
18:37:32.640529  JoinGroup REQUEST  (member_id="")
18:37:32.643549  JoinGroup RESPONSE (error=79, member_id assigned)
18:37:32.643805  JoinGroup REQUEST  (member_id="consumer-xxx-uuid")  ← 3ms later
18:37:32.669109  JoinGroup RESPONSE (SUCCESS, leader=true)
```

## Heartbeat Maintenance

After successful join, the consumer sends heartbeats at regular intervals to maintain group membership.

### Heartbeat Configuration
```go
config.Librd.SetKey("session.timeout.ms", 10000)      // 10 seconds
config.Librd.SetKey("heartbeat.interval.ms", 1000)    // 1 second
```

### Captured Heartbeat Sequence

| Timestamp | Correlation ID | Latency |
|-----------|---------------|---------|
| 18:37:32.699369 | 9 | 2.66ms |
| 18:37:34.643199 | 10 | 4.69ms |
| 18:37:35.645057 | 11 | 6.77ms |
| 18:37:36.646373 | 12 | 3.00ms |
| 18:37:37.647957 | 13 | 2.17ms |
| 18:37:38.651274 | 14 | 2.10ms |
| 18:37:39.656565 | 15 | 3.27ms |
| 18:37:40.657236 | 16 | 5.48ms |
| 18:37:41.658915 | 17 | 1.80ms |
| 18:37:42.663895 | 18 | 1.96ms |

**Observation**: Heartbeats are sent approximately every 1 second (as configured).

## Consumer Shutdown Sequence

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                       Consumer Shutdown Sequence                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  Time          Event                                                          │
│  ──────────────────────────────────────────────────────────────────────────  │
│  18:37:42.699895   Consumer shutting down...                                  │
│  18:37:42.700000   Sleep done (test artifact)                                 │
│  18:37:42.700169   Partitions assigned (delayed due to sleep)                 │
│  18:37:42.700196   Stopping consumer loop                                     │
│  18:37:42.700206   Consumer closing...                                        │
│  18:37:42.700233   OffsetFetch REQUEST (commit offsets)                       │
│  18:37:42.712603   OffsetFetch RESPONSE                                       │
│  18:37:42.712860   Metadata REQUEST                                           │
│  18:37:42.712922   OnPartitionRevoked callback                                │
│  18:37:42.713247   Partitions revoked                                         │
│  18:37:42.713603   Consumer closed                                            │
│  18:37:42.713617   Consumer stopped                                           │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Shutdown Duration
- Total shutdown time: ~14ms (from 18:37:42.699895 to 18:37:42.713617)
- OffsetFetch latency: 12.37ms (majority of shutdown time)

## Timing Analysis

### Full Join Sequence (With 2 Error Injections)

| Phase | Start | End | Duration |
|-------|-------|-----|----------|
| Initial setup | 18:37:26.624 | 18:37:26.637 | 13ms |
| Coordinator discovery | 18:37:26.637 | 18:37:27.636 | ~1s |
| JoinGroup #1 + error | 18:37:27.636 | 18:37:27.639 | 3ms |
| Backoff + retry | 18:37:27.639 | 18:37:30.634 | ~3s |
| JoinGroup #2 + error | 18:37:30.634 | 18:37:30.660 | 26ms |
| Backoff + retry | 18:37:30.660 | 18:37:32.640 | ~2s |
| JoinGroup #3 (KIP-394) | 18:37:32.640 | 18:37:32.643 | 3ms |
| JoinGroup #4 (success) | 18:37:32.643 | 18:37:32.669 | 26ms |
| SyncGroup | 18:37:32.671 | 18:37:32.699 | 28ms |
| **Total join time** | 18:37:26.624 | 18:37:32.699 | **~6s** |

### Without Errors (Estimated)
- Initial setup: ~13ms
- Coordinator discovery: ~1s
- JoinGroup (KIP-394 two-phase): ~30ms
- SyncGroup: ~28ms
- **Expected total**: ~1.1s

### Impact of Errors
Each `COORDINATOR_NOT_AVAILABLE` error adds:
- ~2-3 seconds backoff time
- 2 errors added ~5 seconds to join time

## Callback Timing

### OnPartitionAssigned
Called BEFORE librdkafka's `Assign()` completes:
```
18:37:32.699361  OnPartitionAssigned callback START
18:37:32.699395  Sleep before Assign() (test artifact)
18:37:42.700000  Sleep done
18:37:42.700169  Assign() completed
```

**Note**: In production, `Assign()` is immediate. The 10-second sleep was added for testing visibility.

### OnPartitionRevoked
Called during shutdown after `Unsubscribe()`:
```
18:37:42.712922  OnPartitionRevoked callback
18:37:42.713247  Revoke completed
```

## Error Code Reference

| Code | Name | Behavior |
|------|------|----------|
| 0 | NONE | Success |
| 15 | COORDINATOR_NOT_AVAILABLE | Retry with backoff |
| 16 | NOT_COORDINATOR | Retry with new coordinator |
| 22 | ILLEGAL_GENERATION | Re-join required |
| 25 | UNKNOWN_MEMBER_ID | Re-join required |
| 27 | REBALANCE_IN_PROGRESS | Retry with backoff |
| 79 | MEMBER_ID_REQUIRED | Normal KIP-394 flow |

## Test Command

```bash
go test -v -run "TestGroupConsumer_JoinGroup/JoinGroup_CoordinatorNotAvailable_ShouldRetryAndSucceed" \
    ./kafka/adaptors/librd/... -timeout 180s
```

## State Store Recovery Pattern

### The Problem

When using `OnPartitionAssigned` for state store recovery (e.g., restoring from changelog topics), you need to understand:

1. **When is the rebalance "complete" from Kafka's perspective?**
2. **Will messages start flowing before recovery finishes?**
3. **What are the timeout risks?**

### Protocol vs Application Rebalance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│            Kafka Protocol vs Application-Level Rebalance                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  KAFKA BROKER PERSPECTIVE                                                    │
│  ════════════════════════                                                    │
│  Rebalance complete after SyncGroup response                                 │
│  Consumer is now "stable" member of the group                                │
│                                                                              │
│  APPLICATION PERSPECTIVE (kstream)                                           │
│  ═════════════════════════════════                                           │
│  Rebalance complete after OnPartitionAssigned returns + Assign() called      │
│  State stores recovered, ready to process                                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Execution Flow in group_consumer.go

```go
func (g *groupConsumer) rebalance(c *librdKafka.Consumer, event librdKafka.Event) error {
    // 1. SyncGroup already completed at this point
    //    Broker considers rebalance DONE

    // 2. OnPartitionAssigned runs (YOUR STATE RECOVERY HERE)
    if err := g.groupHandler.OnPartitionAssigned(ctx, session); err != nil {
        return err
    }

    // 3. ONLY NOW does Assign() get called
    //    Message fetching begins AFTER this
    err = c.Assign(librdAssign)
}
```

### Timeline with State Recovery

```
Time          Broker View              Consumer View              Heartbeats
──────────────────────────────────────────────────────────────────────────────
T+0ms         SyncGroup done           Rebalance callback start   -
              REBALANCE COMPLETE ✓

T+1ms         Consumer stable          OnPartitionAssigned()      HB #1 →
                                       State recovery STARTS

T+1000ms      Consumer stable          Recovery in progress...    HB #2 →

T+2000ms      Consumer stable          Recovery in progress...    HB #3 →

...           ...                      ...                        ...

T+30000ms     Consumer stable          Recovery COMPLETE          HB #30 →
                                       Assign() called
                                       Message fetching STARTS
──────────────────────────────────────────────────────────────────────────────
```

### Why This Pattern is Safe

| Concern | Status | Reason |
|---------|--------|--------|
| Messages arriving early | ✅ Safe | `Assign()` not called until after `OnPartitionAssigned` |
| Session timeout | ✅ Safe | Heartbeats continue in background thread |
| Broker kicking consumer | ⚠️ Risk | See timeout considerations below |

### Timeout Considerations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CRITICAL TIMEOUT SETTINGS                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  session.timeout.ms (default: 45000ms)                                       │
│  ├── Broker waits this long for heartbeat before removing consumer           │
│  ├── Heartbeats sent by background thread (NOT blocked by callback)          │
│  └── ✅ Safe: Your callback won't block heartbeats                           │
│                                                                              │
│  max.poll.interval.ms (default: 300000ms = 5 minutes)                        │
│  ├── Maximum time between Poll() calls                                       │
│  ├── Rebalance callback COUNTS against this timeout!                         │
│  ├── If OnPartitionAssigned takes > 5 minutes → consumer kicked              │
│  └── ⚠️ RISK: Long state recovery could exceed this                          │
│                                                                              │
│  RECOMMENDATION:                                                             │
│  If state recovery might take > 5 minutes, increase max.poll.interval.ms:    │
│                                                                              │
│      config.Librd.SetKey("max.poll.interval.ms", 600000)  // 10 minutes      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Observed Behavior (From Test)

With a 10-second simulated state recovery:

```
18:37:32.699142  SyncGroup RESPONSE         ← Broker: rebalance done
18:37:32.699361  OnPartitionAssigned START  ← Recovery begins
18:37:32.699369  Heartbeat #1               ← Session stays alive
18:37:34.643199  Heartbeat #2               ← Still alive
18:37:35.645057  Heartbeat #3               ← Still alive
...
18:37:42.663895  Heartbeat #10              ← Still alive
18:37:42.700169  Assign() called            ← NOW fetching starts
```

**Key observation**: 10 heartbeats sent during the 10-second "recovery" - session remained stable.

### Best Practices

1. **Monitor recovery time** - Track how long `OnPartitionAssigned` takes
2. **Set appropriate timeouts** - Increase `max.poll.interval.ms` if needed
3. **Consider async recovery** - For very long recoveries, consider:
   - Starting recovery in `OnPartitionAssigned`
   - Blocking message processing in `Consume()` until recovery completes
4. **Handle recovery failures** - Return error from `OnPartitionAssigned` to trigger consumer shutdown

### Example: Safe State Recovery Pattern

```go
func (h *MyHandler) OnPartitionAssigned(ctx context.Context, session kafka.GroupSession) error {
    start := time.Now()

    for _, tp := range session.Assignment().TPs() {
        // Restore state store from changelog
        if err := h.stateStore.Restore(tp); err != nil {
            return fmt.Errorf("state recovery failed for %v: %w", tp, err)
        }
    }

    log.Printf("State recovery completed in %v", time.Since(start))

    // Only after this returns will Assign() be called
    // and message fetching will begin
    return nil
}
```