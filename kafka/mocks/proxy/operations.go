package proxy

import "fmt"

// KafkaAPIKey represents Kafka protocol API keys
type KafkaAPIKey int16

// Kafka protocol API keys
// See: https://kafka.apache.org/protocol#protocol_api_keys
const (
	APIKeyProduce                      KafkaAPIKey = 0
	APIKeyFetch                        KafkaAPIKey = 1
	APIKeyListOffsets                  KafkaAPIKey = 2
	APIKeyMetadata                     KafkaAPIKey = 3
	APIKeyLeaderAndIsr                 KafkaAPIKey = 4
	APIKeyStopReplica                  KafkaAPIKey = 5
	APIKeyUpdateMetadata               KafkaAPIKey = 6
	APIKeyControlledShutdown           KafkaAPIKey = 7
	APIKeyOffsetCommit                 KafkaAPIKey = 8
	APIKeyOffsetFetch                  KafkaAPIKey = 9
	APIKeyFindCoordinator              KafkaAPIKey = 10
	APIKeyJoinGroup                    KafkaAPIKey = 11
	APIKeyHeartbeat                    KafkaAPIKey = 12
	APIKeyLeaveGroup                   KafkaAPIKey = 13
	APIKeySyncGroup                    KafkaAPIKey = 14
	APIKeyDescribeGroups               KafkaAPIKey = 15
	APIKeyListGroups                   KafkaAPIKey = 16
	APIKeySaslHandshake                KafkaAPIKey = 17
	APIKeyApiVersions                  KafkaAPIKey = 18
	APIKeyCreateTopics                 KafkaAPIKey = 19
	APIKeyDeleteTopics                 KafkaAPIKey = 20
	APIKeyDeleteRecords                KafkaAPIKey = 21
	APIKeyInitProducerId               KafkaAPIKey = 22
	APIKeyOffsetForLeaderEpoch         KafkaAPIKey = 23
	APIKeyAddPartitionsToTxn           KafkaAPIKey = 24
	APIKeyAddOffsetsToTxn              KafkaAPIKey = 25
	APIKeyEndTxn                       KafkaAPIKey = 26
	APIKeyWriteTxnMarkers              KafkaAPIKey = 27
	APIKeyTxnOffsetCommit              KafkaAPIKey = 28
	APIKeyDescribeAcls                 KafkaAPIKey = 29
	APIKeyCreateAcls                   KafkaAPIKey = 30
	APIKeyDeleteAcls                   KafkaAPIKey = 31
	APIKeyDescribeConfigs              KafkaAPIKey = 32
	APIKeyAlterConfigs                 KafkaAPIKey = 33
	APIKeyAlterReplicaLogDirs          KafkaAPIKey = 34
	APIKeyDescribeLogDirs              KafkaAPIKey = 35
	APIKeySaslAuthenticate             KafkaAPIKey = 36
	APIKeyCreatePartitions             KafkaAPIKey = 37
	APIKeyCreateDelegationToken        KafkaAPIKey = 38
	APIKeyRenewDelegationToken         KafkaAPIKey = 39
	APIKeyExpireDelegationToken        KafkaAPIKey = 40
	APIKeyDescribeDelegationToken      KafkaAPIKey = 41
	APIKeyDeleteGroups                 KafkaAPIKey = 42
	APIKeyElectLeaders                 KafkaAPIKey = 43
	APIKeyIncrementalAlterConfigs      KafkaAPIKey = 44
	APIKeyAlterPartitionReassignments  KafkaAPIKey = 45
	APIKeyListPartitionReassignments   KafkaAPIKey = 46
	APIKeyOffsetDelete                 KafkaAPIKey = 47
	APIKeyDescribeClientQuotas         KafkaAPIKey = 48
	APIKeyAlterClientQuotas            KafkaAPIKey = 49
	APIKeyDescribeUserScramCredentials KafkaAPIKey = 50
	APIKeyAlterUserScramCredentials    KafkaAPIKey = 51
	APIKeyAlterPartition               KafkaAPIKey = 56
	APIKeyUpdateFeatures               KafkaAPIKey = 57
	APIKeyDescribeCluster              KafkaAPIKey = 60
	APIKeyDescribeProducers            KafkaAPIKey = 61
	APIKeyDescribeTransactions         KafkaAPIKey = 65
	APIKeyListTransactions             KafkaAPIKey = 66
	APIKeyAllocateProducerIds          KafkaAPIKey = 67
)

// apiKeyNames maps API keys to their human-readable names
var apiKeyNames = map[KafkaAPIKey]string{
	APIKeyProduce:                      "Produce",
	APIKeyFetch:                        "Fetch",
	APIKeyListOffsets:                  "ListOffsets",
	APIKeyMetadata:                     "Metadata",
	APIKeyLeaderAndIsr:                 "LeaderAndIsr",
	APIKeyStopReplica:                  "StopReplica",
	APIKeyUpdateMetadata:               "UpdateMetadata",
	APIKeyControlledShutdown:           "ControlledShutdown",
	APIKeyOffsetCommit:                 "OffsetCommit",
	APIKeyOffsetFetch:                  "OffsetFetch",
	APIKeyFindCoordinator:              "FindCoordinator",
	APIKeyJoinGroup:                    "JoinGroup",
	APIKeyHeartbeat:                    "Heartbeat",
	APIKeyLeaveGroup:                   "LeaveGroup",
	APIKeySyncGroup:                    "SyncGroup",
	APIKeyDescribeGroups:               "DescribeGroups",
	APIKeyListGroups:                   "ListGroups",
	APIKeySaslHandshake:                "SaslHandshake",
	APIKeyApiVersions:                  "ApiVersions",
	APIKeyCreateTopics:                 "CreateTopics",
	APIKeyDeleteTopics:                 "DeleteTopics",
	APIKeyDeleteRecords:                "DeleteRecords",
	APIKeyInitProducerId:               "InitProducerId",
	APIKeyOffsetForLeaderEpoch:         "OffsetForLeaderEpoch",
	APIKeyAddPartitionsToTxn:           "AddPartitionsToTxn",
	APIKeyAddOffsetsToTxn:              "AddOffsetsToTxn",
	APIKeyEndTxn:                       "EndTxn",
	APIKeyWriteTxnMarkers:              "WriteTxnMarkers",
	APIKeyTxnOffsetCommit:              "TxnOffsetCommit",
	APIKeyDescribeAcls:                 "DescribeAcls",
	APIKeyCreateAcls:                   "CreateAcls",
	APIKeyDeleteAcls:                   "DeleteAcls",
	APIKeyDescribeConfigs:              "DescribeConfigs",
	APIKeyAlterConfigs:                 "AlterConfigs",
	APIKeyAlterReplicaLogDirs:          "AlterReplicaLogDirs",
	APIKeyDescribeLogDirs:              "DescribeLogDirs",
	APIKeySaslAuthenticate:             "SaslAuthenticate",
	APIKeyCreatePartitions:             "CreatePartitions",
	APIKeyCreateDelegationToken:        "CreateDelegationToken",
	APIKeyRenewDelegationToken:         "RenewDelegationToken",
	APIKeyExpireDelegationToken:        "ExpireDelegationToken",
	APIKeyDescribeDelegationToken:      "DescribeDelegationToken",
	APIKeyDeleteGroups:                 "DeleteGroups",
	APIKeyElectLeaders:                 "ElectLeaders",
	APIKeyIncrementalAlterConfigs:      "IncrementalAlterConfigs",
	APIKeyAlterPartitionReassignments:  "AlterPartitionReassignments",
	APIKeyListPartitionReassignments:   "ListPartitionReassignments",
	APIKeyOffsetDelete:                 "OffsetDelete",
	APIKeyDescribeClientQuotas:         "DescribeClientQuotas",
	APIKeyAlterClientQuotas:            "AlterClientQuotas",
	APIKeyDescribeUserScramCredentials: "DescribeUserScramCredentials",
	APIKeyAlterUserScramCredentials:    "AlterUserScramCredentials",
	APIKeyAlterPartition:               "AlterPartition",
	APIKeyUpdateFeatures:               "UpdateFeatures",
	APIKeyDescribeCluster:              "DescribeCluster",
	APIKeyDescribeProducers:            "DescribeProducers",
	APIKeyDescribeTransactions:         "DescribeTransactions",
	APIKeyListTransactions:             "ListTransactions",
	APIKeyAllocateProducerIds:          "AllocateProducerIds",
}

// String returns the human-readable name for an API key
func (k KafkaAPIKey) String() string {
	if name, ok := apiKeyNames[k]; ok {
		return name
	}
	return fmt.Sprintf("APIKey(%d)", k)
}

// IsTransactional returns true if the API key is related to transactions
func (k KafkaAPIKey) IsTransactional() bool {
	switch k {
	case APIKeyInitProducerId,
		APIKeyAddPartitionsToTxn,
		APIKeyAddOffsetsToTxn,
		APIKeyEndTxn,
		APIKeyWriteTxnMarkers,
		APIKeyTxnOffsetCommit:
		return true
	default:
		return false
	}
}

// IsConsumerGroup returns true if the API key is related to consumer groups
func (k KafkaAPIKey) IsConsumerGroup() bool {
	switch k {
	case APIKeyFindCoordinator,
		APIKeyJoinGroup,
		APIKeyHeartbeat,
		APIKeyLeaveGroup,
		APIKeySyncGroup,
		APIKeyDescribeGroups,
		APIKeyListGroups,
		APIKeyDeleteGroups,
		APIKeyOffsetCommit,
		APIKeyOffsetFetch:
		return true
	default:
		return false
	}
}