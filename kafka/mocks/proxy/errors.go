package proxy

import "fmt"

// KafkaErrorCode represents Kafka protocol error codes
type KafkaErrorCode int16

// Kafka protocol error codes
// See: https://kafka.apache.org/protocol#protocol_error_codes
const (
	ErrNone                             KafkaErrorCode = 0
	ErrUnknownServerError               KafkaErrorCode = -1
	ErrOffsetOutOfRange                 KafkaErrorCode = 1
	ErrCorruptMessage                   KafkaErrorCode = 2
	ErrUnknownTopicOrPartition          KafkaErrorCode = 3
	ErrInvalidFetchSize                 KafkaErrorCode = 4
	ErrLeaderNotAvailable               KafkaErrorCode = 5
	ErrNotLeaderForPartition            KafkaErrorCode = 6
	ErrRequestTimedOut                  KafkaErrorCode = 7
	ErrBrokerNotAvailable               KafkaErrorCode = 8
	ErrReplicaNotAvailable              KafkaErrorCode = 9
	ErrMessageTooLarge                  KafkaErrorCode = 10
	ErrStaleControllerEpoch             KafkaErrorCode = 11
	ErrOffsetMetadataTooLarge           KafkaErrorCode = 12
	ErrNetworkException                 KafkaErrorCode = 13
	ErrCoordinatorLoadInProgress        KafkaErrorCode = 14
	ErrCoordinatorNotAvailable          KafkaErrorCode = 15
	ErrNotCoordinator                   KafkaErrorCode = 16
	ErrInvalidTopicException            KafkaErrorCode = 17
	ErrRecordListTooLarge               KafkaErrorCode = 18
	ErrNotEnoughReplicas                KafkaErrorCode = 19
	ErrNotEnoughReplicasAfterAppend     KafkaErrorCode = 20
	ErrInvalidRequiredAcks              KafkaErrorCode = 21
	ErrIllegalGeneration                KafkaErrorCode = 22
	ErrInconsistentGroupProtocol        KafkaErrorCode = 23
	ErrInvalidGroupID                   KafkaErrorCode = 24
	ErrUnknownMemberID                  KafkaErrorCode = 25
	ErrInvalidSessionTimeout            KafkaErrorCode = 26
	ErrRebalanceInProgress              KafkaErrorCode = 27
	ErrInvalidCommitOffsetSize          KafkaErrorCode = 28
	ErrTopicAuthorizationFailed         KafkaErrorCode = 29
	ErrGroupAuthorizationFailed         KafkaErrorCode = 30
	ErrClusterAuthorizationFailed       KafkaErrorCode = 31
	ErrInvalidTimestamp                 KafkaErrorCode = 32
	ErrUnsupportedSaslMechanism         KafkaErrorCode = 33
	ErrIllegalSaslState                 KafkaErrorCode = 34
	ErrUnsupportedVersion               KafkaErrorCode = 35
	ErrTopicAlreadyExists               KafkaErrorCode = 36
	ErrInvalidPartitions                KafkaErrorCode = 37
	ErrInvalidReplicationFactor         KafkaErrorCode = 38
	ErrInvalidReplicaAssignment         KafkaErrorCode = 39
	ErrInvalidConfig                    KafkaErrorCode = 40
	ErrNotController                    KafkaErrorCode = 41
	ErrInvalidRequest                   KafkaErrorCode = 42
	ErrUnsupportedForMessageFormat      KafkaErrorCode = 43
	ErrPolicyViolation                  KafkaErrorCode = 44
	ErrOutOfOrderSequenceNumber         KafkaErrorCode = 45
	ErrDuplicateSequenceNumber          KafkaErrorCode = 46
	ErrInvalidProducerEpoch             KafkaErrorCode = 47
	ErrInvalidTxnState                  KafkaErrorCode = 48
	ErrInvalidProducerIDMapping         KafkaErrorCode = 49
	ErrInvalidTransactionTimeout        KafkaErrorCode = 50
	ErrConcurrentTransactions           KafkaErrorCode = 51
	ErrTransactionCoordinatorFenced     KafkaErrorCode = 52
	ErrTransactionalIDAuthorizationFail KafkaErrorCode = 53
	ErrSecurityDisabled                 KafkaErrorCode = 54
	ErrOperationNotAttempted            KafkaErrorCode = 55
	ErrKafkaStorageError                KafkaErrorCode = 56
	ErrLogDirNotFound                   KafkaErrorCode = 57
	ErrSaslAuthenticationFailed         KafkaErrorCode = 58
	ErrUnknownProducerID                KafkaErrorCode = 59
	ErrReassignmentInProgress           KafkaErrorCode = 60
	ErrDelegationTokenAuthDisabled      KafkaErrorCode = 61
	ErrDelegationTokenNotFound          KafkaErrorCode = 62
	ErrDelegationTokenOwnerMismatch     KafkaErrorCode = 63
	ErrDelegationTokenRequestNotAllowed KafkaErrorCode = 64
	ErrDelegationTokenAuthorizationFail KafkaErrorCode = 65
	ErrDelegationTokenExpired           KafkaErrorCode = 66
	ErrInvalidPrincipalType             KafkaErrorCode = 67
	ErrNonEmptyGroup                    KafkaErrorCode = 68
	ErrGroupIDNotFound                  KafkaErrorCode = 69
	ErrFetchSessionIDNotFound           KafkaErrorCode = 70
	ErrInvalidFetchSessionEpoch         KafkaErrorCode = 71
	ErrListenerNotFound                 KafkaErrorCode = 72
	ErrTopicDeletionDisabled            KafkaErrorCode = 73
	ErrFencedLeaderEpoch                KafkaErrorCode = 74
	ErrUnknownLeaderEpoch               KafkaErrorCode = 75
	ErrUnsupportedCompressionType       KafkaErrorCode = 76
	ErrStaleBrokerEpoch                 KafkaErrorCode = 77
	ErrOffsetNotAvailable               KafkaErrorCode = 78
	ErrMemberIDRequired                 KafkaErrorCode = 79
	ErrPreferredLeaderNotAvailable      KafkaErrorCode = 80
	ErrGroupMaxSizeReached              KafkaErrorCode = 81
	ErrFencedInstanceID                 KafkaErrorCode = 82
	ErrEligibleLeadersNotAvailable      KafkaErrorCode = 83
	ErrElectionNotNeeded                KafkaErrorCode = 84
	ErrNoReassignmentInProgress         KafkaErrorCode = 85
	ErrGroupSubscribedToTopic           KafkaErrorCode = 86
	ErrInvalidRecord                    KafkaErrorCode = 87
	ErrUnstableOffsetCommit             KafkaErrorCode = 88
	ErrThrottlingQuotaExceeded          KafkaErrorCode = 89
	ErrProducerFenced                   KafkaErrorCode = 90
	ErrResourceNotFound                 KafkaErrorCode = 91
	ErrDuplicateResource                KafkaErrorCode = 92
	ErrUnacceptableCredential           KafkaErrorCode = 93
	ErrInconsistentVoterSet             KafkaErrorCode = 94
	ErrInvalidUpdateVersion             KafkaErrorCode = 95
	ErrFeatureUpdateFailed              KafkaErrorCode = 96
	ErrPrincipalDeserializationFailure  KafkaErrorCode = 97
	ErrSnapshotNotFound                 KafkaErrorCode = 98
	ErrPositionOutOfRange               KafkaErrorCode = 99
	ErrUnknownTopicID                   KafkaErrorCode = 100
	ErrDuplicateBrokerRegistration      KafkaErrorCode = 101
	ErrBrokerIDNotRegistered            KafkaErrorCode = 102
	ErrInconsistentTopicID              KafkaErrorCode = 103
	ErrInconsistentClusterID            KafkaErrorCode = 104
	ErrTransactionalIDNotFound          KafkaErrorCode = 105
	ErrFetchSessionTopicIDError         KafkaErrorCode = 106
)

// errorCodeNames maps error codes to their human-readable names
var errorCodeNames = map[KafkaErrorCode]string{
	ErrNone:                             "NONE",
	ErrUnknownServerError:               "UNKNOWN_SERVER_ERROR",
	ErrOffsetOutOfRange:                 "OFFSET_OUT_OF_RANGE",
	ErrCorruptMessage:                   "CORRUPT_MESSAGE",
	ErrUnknownTopicOrPartition:          "UNKNOWN_TOPIC_OR_PARTITION",
	ErrInvalidFetchSize:                 "INVALID_FETCH_SIZE",
	ErrLeaderNotAvailable:               "LEADER_NOT_AVAILABLE",
	ErrNotLeaderForPartition:            "NOT_LEADER_FOR_PARTITION",
	ErrRequestTimedOut:                  "REQUEST_TIMED_OUT",
	ErrBrokerNotAvailable:               "BROKER_NOT_AVAILABLE",
	ErrReplicaNotAvailable:              "REPLICA_NOT_AVAILABLE",
	ErrMessageTooLarge:                  "MESSAGE_TOO_LARGE",
	ErrStaleControllerEpoch:             "STALE_CONTROLLER_EPOCH",
	ErrOffsetMetadataTooLarge:           "OFFSET_METADATA_TOO_LARGE",
	ErrNetworkException:                 "NETWORK_EXCEPTION",
	ErrCoordinatorLoadInProgress:        "COORDINATOR_LOAD_IN_PROGRESS",
	ErrCoordinatorNotAvailable:          "COORDINATOR_NOT_AVAILABLE",
	ErrNotCoordinator:                   "NOT_COORDINATOR",
	ErrInvalidTopicException:            "INVALID_TOPIC_EXCEPTION",
	ErrRecordListTooLarge:               "RECORD_LIST_TOO_LARGE",
	ErrNotEnoughReplicas:                "NOT_ENOUGH_REPLICAS",
	ErrNotEnoughReplicasAfterAppend:     "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
	ErrInvalidRequiredAcks:              "INVALID_REQUIRED_ACKS",
	ErrIllegalGeneration:                "ILLEGAL_GENERATION",
	ErrInconsistentGroupProtocol:        "INCONSISTENT_GROUP_PROTOCOL",
	ErrInvalidGroupID:                   "INVALID_GROUP_ID",
	ErrUnknownMemberID:                  "UNKNOWN_MEMBER_ID",
	ErrInvalidSessionTimeout:            "INVALID_SESSION_TIMEOUT",
	ErrRebalanceInProgress:              "REBALANCE_IN_PROGRESS",
	ErrInvalidCommitOffsetSize:          "INVALID_COMMIT_OFFSET_SIZE",
	ErrTopicAuthorizationFailed:         "TOPIC_AUTHORIZATION_FAILED",
	ErrGroupAuthorizationFailed:         "GROUP_AUTHORIZATION_FAILED",
	ErrClusterAuthorizationFailed:       "CLUSTER_AUTHORIZATION_FAILED",
	ErrInvalidTimestamp:                 "INVALID_TIMESTAMP",
	ErrUnsupportedSaslMechanism:         "UNSUPPORTED_SASL_MECHANISM",
	ErrIllegalSaslState:                 "ILLEGAL_SASL_STATE",
	ErrUnsupportedVersion:               "UNSUPPORTED_VERSION",
	ErrTopicAlreadyExists:               "TOPIC_ALREADY_EXISTS",
	ErrInvalidPartitions:                "INVALID_PARTITIONS",
	ErrInvalidReplicationFactor:         "INVALID_REPLICATION_FACTOR",
	ErrInvalidReplicaAssignment:         "INVALID_REPLICA_ASSIGNMENT",
	ErrInvalidConfig:                    "INVALID_CONFIG",
	ErrNotController:                    "NOT_CONTROLLER",
	ErrInvalidRequest:                   "INVALID_REQUEST",
	ErrUnsupportedForMessageFormat:      "UNSUPPORTED_FOR_MESSAGE_FORMAT",
	ErrPolicyViolation:                  "POLICY_VIOLATION",
	ErrOutOfOrderSequenceNumber:         "OUT_OF_ORDER_SEQUENCE_NUMBER",
	ErrDuplicateSequenceNumber:          "DUPLICATE_SEQUENCE_NUMBER",
	ErrInvalidProducerEpoch:             "INVALID_PRODUCER_EPOCH",
	ErrInvalidTxnState:                  "INVALID_TXN_STATE",
	ErrInvalidProducerIDMapping:         "INVALID_PRODUCER_ID_MAPPING",
	ErrInvalidTransactionTimeout:        "INVALID_TRANSACTION_TIMEOUT",
	ErrConcurrentTransactions:           "CONCURRENT_TRANSACTIONS",
	ErrTransactionCoordinatorFenced:     "TRANSACTION_COORDINATOR_FENCED",
	ErrTransactionalIDAuthorizationFail: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
	ErrSecurityDisabled:                 "SECURITY_DISABLED",
	ErrOperationNotAttempted:            "OPERATION_NOT_ATTEMPTED",
	ErrKafkaStorageError:                "KAFKA_STORAGE_ERROR",
	ErrLogDirNotFound:                   "LOG_DIR_NOT_FOUND",
	ErrSaslAuthenticationFailed:         "SASL_AUTHENTICATION_FAILED",
	ErrUnknownProducerID:                "UNKNOWN_PRODUCER_ID",
	ErrReassignmentInProgress:           "REASSIGNMENT_IN_PROGRESS",
	ErrDelegationTokenAuthDisabled:      "DELEGATION_TOKEN_AUTH_DISABLED",
	ErrDelegationTokenNotFound:          "DELEGATION_TOKEN_NOT_FOUND",
	ErrDelegationTokenOwnerMismatch:     "DELEGATION_TOKEN_OWNER_MISMATCH",
	ErrDelegationTokenRequestNotAllowed: "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
	ErrDelegationTokenAuthorizationFail: "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
	ErrDelegationTokenExpired:           "DELEGATION_TOKEN_EXPIRED",
	ErrInvalidPrincipalType:             "INVALID_PRINCIPAL_TYPE",
	ErrNonEmptyGroup:                    "NON_EMPTY_GROUP",
	ErrGroupIDNotFound:                  "GROUP_ID_NOT_FOUND",
	ErrFetchSessionIDNotFound:           "FETCH_SESSION_ID_NOT_FOUND",
	ErrInvalidFetchSessionEpoch:         "INVALID_FETCH_SESSION_EPOCH",
	ErrListenerNotFound:                 "LISTENER_NOT_FOUND",
	ErrTopicDeletionDisabled:            "TOPIC_DELETION_DISABLED",
	ErrFencedLeaderEpoch:                "FENCED_LEADER_EPOCH",
	ErrUnknownLeaderEpoch:               "UNKNOWN_LEADER_EPOCH",
	ErrUnsupportedCompressionType:       "UNSUPPORTED_COMPRESSION_TYPE",
	ErrStaleBrokerEpoch:                 "STALE_BROKER_EPOCH",
	ErrOffsetNotAvailable:               "OFFSET_NOT_AVAILABLE",
	ErrMemberIDRequired:                 "MEMBER_ID_REQUIRED",
	ErrPreferredLeaderNotAvailable:      "PREFERRED_LEADER_NOT_AVAILABLE",
	ErrGroupMaxSizeReached:              "GROUP_MAX_SIZE_REACHED",
	ErrFencedInstanceID:                 "FENCED_INSTANCE_ID",
	ErrEligibleLeadersNotAvailable:      "ELIGIBLE_LEADERS_NOT_AVAILABLE",
	ErrElectionNotNeeded:                "ELECTION_NOT_NEEDED",
	ErrNoReassignmentInProgress:         "NO_REASSIGNMENT_IN_PROGRESS",
	ErrGroupSubscribedToTopic:           "GROUP_SUBSCRIBED_TO_TOPIC",
	ErrInvalidRecord:                    "INVALID_RECORD",
	ErrUnstableOffsetCommit:             "UNSTABLE_OFFSET_COMMIT",
	ErrThrottlingQuotaExceeded:          "THROTTLING_QUOTA_EXCEEDED",
	ErrProducerFenced:                   "PRODUCER_FENCED",
	ErrResourceNotFound:                 "RESOURCE_NOT_FOUND",
	ErrDuplicateResource:                "DUPLICATE_RESOURCE",
	ErrUnacceptableCredential:           "UNACCEPTABLE_CREDENTIAL",
	ErrInconsistentVoterSet:             "INCONSISTENT_VOTER_SET",
	ErrInvalidUpdateVersion:             "INVALID_UPDATE_VERSION",
	ErrFeatureUpdateFailed:              "FEATURE_UPDATE_FAILED",
	ErrPrincipalDeserializationFailure:  "PRINCIPAL_DESERIALIZATION_FAILURE",
	ErrSnapshotNotFound:                 "SNAPSHOT_NOT_FOUND",
	ErrPositionOutOfRange:               "POSITION_OUT_OF_RANGE",
	ErrUnknownTopicID:                   "UNKNOWN_TOPIC_ID",
	ErrDuplicateBrokerRegistration:      "DUPLICATE_BROKER_REGISTRATION",
	ErrBrokerIDNotRegistered:            "BROKER_ID_NOT_REGISTERED",
	ErrInconsistentTopicID:              "INCONSISTENT_TOPIC_ID",
	ErrInconsistentClusterID:            "INCONSISTENT_CLUSTER_ID",
	ErrTransactionalIDNotFound:          "TRANSACTIONAL_ID_NOT_FOUND",
	ErrFetchSessionTopicIDError:         "FETCH_SESSION_TOPIC_ID_ERROR",
}

// String returns the human-readable name for an error code
func (e KafkaErrorCode) String() string {
	if name, ok := errorCodeNames[e]; ok {
		return name
	}
	return fmt.Sprintf("ERROR_%d", e)
}

// Int16 returns the error code as int16 for use with Kafka protocol
func (e KafkaErrorCode) Int16() int16 {
	return int16(e)
}

// IsRetriable returns true if the error is retriable
// These are transient errors where retrying the operation may succeed
func (e KafkaErrorCode) IsRetriable() bool {
	switch e {
	case ErrCorruptMessage,
		ErrUnknownTopicOrPartition,
		ErrLeaderNotAvailable,
		ErrNotLeaderForPartition,
		ErrRequestTimedOut,
		ErrReplicaNotAvailable,
		ErrNetworkException,
		ErrCoordinatorLoadInProgress,
		ErrCoordinatorNotAvailable,
		ErrNotCoordinator,
		ErrNotEnoughReplicas,
		ErrNotEnoughReplicasAfterAppend,
		ErrNotController,
		ErrKafkaStorageError,
		ErrFetchSessionIDNotFound,
		ErrListenerNotFound,
		ErrFencedLeaderEpoch,
		ErrUnknownLeaderEpoch,
		ErrOffsetNotAvailable,
		ErrPreferredLeaderNotAvailable,
		ErrEligibleLeadersNotAvailable,
		ErrUnstableOffsetCommit,
		ErrThrottlingQuotaExceeded:
		return true
	default:
		return false
	}
}

// IsTransactionAbortable returns true if this error requires aborting the current transaction
// The transaction cannot be committed and must be aborted before starting a new one
func (e KafkaErrorCode) IsTransactionAbortable() bool {
	switch e {
	case ErrOutOfOrderSequenceNumber,
		ErrDuplicateSequenceNumber,
		ErrInvalidProducerEpoch,
		ErrInvalidTxnState,
		ErrInvalidProducerIDMapping,
		ErrTransactionCoordinatorFenced,
		ErrConcurrentTransactions:
		return true
	default:
		return false
	}
}

// IsFatal returns true if this error is fatal and requires producer restart
// The producer instance is no longer usable and must be recreated
func (e KafkaErrorCode) IsFatal() bool {
	switch e {
	case ErrProducerFenced,
		ErrTransactionalIDAuthorizationFail,
		ErrClusterAuthorizationFailed,
		ErrTopicAuthorizationFailed,
		ErrGroupAuthorizationFailed,
		ErrInvalidProducerEpoch,
		ErrFencedInstanceID:
		return true
	default:
		return false
	}
}

// IsTransactional returns true if this error is specific to transactional operations
func (e KafkaErrorCode) IsTransactional() bool {
	switch e {
	case ErrOutOfOrderSequenceNumber,
		ErrDuplicateSequenceNumber,
		ErrInvalidProducerEpoch,
		ErrInvalidTxnState,
		ErrInvalidProducerIDMapping,
		ErrInvalidTransactionTimeout,
		ErrConcurrentTransactions,
		ErrTransactionCoordinatorFenced,
		ErrTransactionalIDAuthorizationFail,
		ErrProducerFenced,
		ErrUnknownProducerID,
		ErrTransactionalIDNotFound:
		return true
	default:
		return false
	}
}