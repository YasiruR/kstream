package mocks

import (
	"strings"
	"testing"
	"time"

	librdKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

func createConsumerWithIsolation(bootstrapServers []string, topic, groupID, isolationLevel string) (*librdKafka.Consumer, error) {
	config := &librdKafka.ConfigMap{
		"bootstrap.servers":    strings.Join(bootstrapServers, `,`),
		"group.id":             groupID,
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
	deadline := time.Now().Add(timeout)
	pollTimeoutMs := 1000 // 1 second poll intervals

	for time.Now().Before(deadline) {
		ev := consumer.Poll(pollTimeoutMs)
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
			return false, ""
		case *librdKafka.Error:
			// Log but continue polling
			continue
		default:
			_ = e // ignore other events
		}
	}
	return false, ""
}

// ConsumeMessageReadCommitted consumes a message with read_committed isolation level
func ConsumeMessageReadCommitted(t *testing.T, bootstrapServers []string, topic, targetKey string, timeout time.Duration) (found bool, key string) {
	consumer, err := createConsumerWithIsolation(bootstrapServers, topic, uuid.NewString(), "read_committed")
	if err != nil {
		t.Logf("Failed to create consumer: %v", err)
		return false, ""
	}
	defer consumer.Close()

	found, key = consumeMessage(consumer, targetKey, timeout)
	return found, key
}

// ConsumeMessageReadUncommitted consumes a message with read_uncommitted isolation level
func ConsumeMessageReadUncommitted(t *testing.T, bootstrapServers []string, topic, targetKey string, timeout time.Duration) (found bool, key string) {
	consumer, err := createConsumerWithIsolation(bootstrapServers, topic, uuid.NewString(), "read_uncommitted")
	if err != nil {
		t.Logf("Failed to create consumer: %v", err)
		return false, ""
	}
	defer consumer.Close()

	found, key = consumeMessage(consumer, targetKey, timeout)
	return found, key
}