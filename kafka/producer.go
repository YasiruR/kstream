/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type DeliveryReport interface {
	TopicPartition() TopicPartition
	Offset() Offset
	Error() error
	Delivered() bool
}

type ProducerErr interface {
	error
	RequiresRestart() bool
	TxnRequiresAbort() bool
	ShouldShutdown() bool
	Code() int
}

type ProducerProvider interface {
	NewBuilder(config *ProducerConfig) ProducerBuilder
}

type ProducerBuilder func(conf func(*ProducerConfig)) (Producer, error)

type RequiredAcks int

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0

	// WaitForLeader waits for only the local commit to succeed before responding.
	WaitForLeader RequiredAcks = 1

	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = -1
)

const PartitionAny = -1

func (ack RequiredAcks) String() string {
	a := `NoResponse`

	if ack == WaitForLeader {
		a = `WaitForLeader`
	}

	if ack == WaitForAll {
		a = `WaitForAll`
	}

	return a
}

type ConsumerOffset struct {
	Topic     string
	Partition int32
	Offset    Offset
	Meta      string
}

func (off *ConsumerOffset) String() string {
	return fmt.Sprintf(`%s@%d%d`, off.Topic, off.Partition, off.Offset)
}

type ConsumerOffsets []ConsumerOffset

func (list ConsumerOffsets) Print() string {
	str := strings.Builder{}

	var current string
	for i, tp := range list {
		if current != tp.Topic {
			//if current == ``{
			//	str.WriteString(tp.Topic+ `: `)
			//}else {
			//	str.WriteString("\n"+tp.Topic+ `: `)
			//}
			str.WriteString("\n" + tp.Topic + `: `)
			current = tp.Topic
		}

		if i != len(list)-1 && list[i+1].Topic != current {
			str.WriteString(fmt.Sprintf("%d@%s", tp.Partition, tp.Offset))
		} else {
			str.WriteString(fmt.Sprintf("%d@%s, ", tp.Partition, tp.Offset))
		}
	}

	return str.String()
}

type Producer interface {
	NewRecord(
		ctx context.Context,
		key, value []byte,
		topic string,
		partition int32,
		timestamp time.Time,
		headers RecordHeaders,
		meta string) Record
	ProduceSync(ctx context.Context, record Record) (partition int32, offset int64, err error)
	Flush()
	Restart() error
	Close() error
}

type PartitionerFunc func(record Record, numPartitions int32) (int32, error)

type PartitionerType string

type TransactionalProducer interface {
	Producer
	ProduceAsync(ctx context.Context, record Record) (err error)
	InitTransactions(ctx context.Context) error
	BeginTransaction() error
	SendOffsetsToTransaction(ctx context.Context, offsets []ConsumerOffset, meta *GroupMeta) error
	CommitTransaction(ctx context.Context) error
	AbortTransaction(ctx context.Context) error
}
