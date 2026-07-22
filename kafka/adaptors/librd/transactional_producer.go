/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package librd

import (
	"context"
	"errors"
	"fmt"
	"time"

	librdKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka"
	kstreamErrors "github.com/gmbyapa/kstream/v2/pkg/errors"
)

// Note:: do we need both fatal and shutdown types?
type Err struct {
	error
	restart        bool
	shouldAbort    bool
	shouldShutdown bool
	code           int
}

func (e Err) TxnRequiresAbort() bool {
	return e.shouldAbort
}

func (e Err) RequiresRestart() bool {
	return e.restart
}

func (e Err) ShouldShutdown() bool {
	return e.shouldShutdown
}

func (e Err) Code() int {
	return e.code
}

const (
	transactionErrorPhaseInit        = `InitFailed`
	transactionErrorPhaseBegin       = `BeginFailed`
	transactionErrorPhaseSend        = `SendFailed`
	transactionErrorPhaseSendOffsets = `SendOffsetsFailed`
	transactionErrorPhaseCommit      = `CommitFailed`
	transactionErrorPhaseAbort       = `AbortFailed`
)

type TransactionalProducer struct {
	*Producer
	txBegin bool
}

func (p *TransactionalProducer) InitTransactions(ctx context.Context) error {
	defer func(begin time.Time) {
		p.metrics.transactions.initLatency.Observe(float64(time.Since(begin).Microseconds()), nil)
	}(time.Now())

	if err := p.Producer.baseProducer.InitTransactions(ctx); err != nil {
		return p.handleTxError(ctx, err, errTxInit, func() error {
			return p.Producer.baseProducer.InitTransactions(ctx)
		}, 0)
	}

	p.config.Logger.Info(`Transaction inited`)

	return nil
}

func (p *TransactionalProducer) BeginTransaction() error {
	if err := p.Producer.baseProducer.BeginTransaction(); err != nil {
		return p.handleTxError(context.Background(), err, errTxBegin, func() error {
			return p.Producer.baseProducer.BeginTransaction()
		}, 0)
	}

	p.txBegin = true

	return nil
}

func (p *TransactionalProducer) CommitTransaction(ctx context.Context) error {
	defer func(begin time.Time) {
		p.metrics.transactions.commitLatency.Observe(float64(time.Since(begin).Microseconds()), nil)
	}(time.Now())

	defer p.resetState()

	if err := p.Producer.baseProducer.CommitTransaction(ctx); err != nil {
		return p.handleTxError(ctx, err, errTxCommit, func() error {
			return p.Producer.baseProducer.CommitTransaction(ctx)
		}, 0)
	}

	p.Producer.config.Logger.Trace(fmt.Sprintf(`transaction commited`))

	return nil
}

func (p *TransactionalProducer) SendOffsetsToTransaction(ctx context.Context, offsets []kafka.ConsumerOffset, meta *kafka.GroupMeta) error {
	var kOffsets []librdKafka.TopicPartition
	for i := range offsets {
		kOffsets = append(kOffsets, librdKafka.TopicPartition{
			Topic:     &offsets[i].Topic,
			Partition: offsets[i].Partition,
			Offset:    librdKafka.Offset(offsets[i].Offset),
			Metadata:  &offsets[i].Meta,
		})
	}

	if !p.txBegin {
		if err := p.BeginTransaction(); err != nil {
			panic(err)
		}
	}

	cgMeta := meta.Meta.(*librdKafka.ConsumerGroupMetadata)
	if err := p.Producer.baseProducer.SendOffsetsToTransaction(ctx, kOffsets, cgMeta); err != nil {
		return p.handleTxError(ctx, err, errTxSendOffsets, func() error {
			return p.Producer.baseProducer.SendOffsetsToTransaction(ctx, kOffsets, cgMeta)
		}, 0)
	}

	p.Producer.config.Logger.Trace(fmt.Sprintf(`Offsets sent, %+v`, kOffsets))

	return nil
}

func (p *TransactionalProducer) AbortTransaction(ctx context.Context) error {
	defer p.resetState()

	defer func(begin time.Time) {
		p.metrics.transactions.abortLatency.Observe(float64(time.Since(begin).Microseconds()), nil)
	}(time.Now())

	if err := p.Producer.baseProducer.AbortTransaction(ctx); err != nil && err.(librdKafka.Error).Code() != librdKafka.ErrState {
		return p.handleTxError(ctx, err, errTxAbort, func() error {
			return p.Producer.baseProducer.AbortTransaction(ctx)
		}, 0)
	}

	p.config.Logger.WarnContext(ctx, fmt.Sprintf(`Transaction aborted`))

	return nil
}

func (p *TransactionalProducer) ProduceSync(ctx context.Context, message kafka.Record) (partition int32, offset int64, err error) {
	panic(`transactional producer does not support ProduceSync mode`)
}

func (p *TransactionalProducer) ProduceAsync(ctx context.Context, message kafka.Record) (err error) {
	defer func(begin time.Time) {
		p.metrics.produceLatency.Observe(float64(time.Since(begin).Microseconds()), map[string]string{
			`topic`: message.Topic(),
		})
	}(time.Now())

	if !p.txBegin {
		if err := p.BeginTransaction(); err != nil {
			return err
		}
	}

	kMessage, err := p.prepareMessage(message)
	if err != nil {
		return p.handleTxError(ctx, err, errProduce, nil, 0)
	}

	err = p.Producer.baseProducer.Produce(kMessage, nil)
	if err != nil {
		return p.handleTxError(ctx, err, errProduce, func() error {
			return p.Producer.baseProducer.Produce(kMessage, nil)
		}, 0)
	}

	p.config.Logger.TraceContext(ctx, "Record "+message.String()+" queued")

	return nil
}

type errorType string

const (
	errProduce       errorType = `send failed`
	errTxInit        errorType = `transaction init failed`
	errTxBegin       errorType = `transaction begin failed`
	errTxSendOffsets errorType = `transaction send offests failed`
	errTxCommit      errorType = `transaction commit failed`
	errTxAbort       errorType = `transaction abort failed`
)

func (p *TransactionalProducer) handleTxError(ctx context.Context, err error, errorType errorType, retryOp func() error, numOfAttempts int) error {
	p.metrics.produceErrors.Count(1, map[string]string{`error`: fmt.Sprint(err)})

	// Try to extract librdKafka.Error from potentially wrapped errors
	var librdErr librdKafka.Error
	if !errors.As(err, &librdErr) {
		// If we can't extract a librdKafka.Error, treat as fatal (This shouldn't happen unless there a bug)
		p.config.Logger.ErrorContext(ctx, fmt.Sprintf(`Unknown error type, cannot classify: %T - %v`, err, err))
		p.resetState()
		return Err{
			error:          err,
			shouldShutdown: true,
		}
	}

	if numOfAttempts > p.Producer.config.MaxRetryCount {
		p.config.Logger.ErrorContext(ctx, fmt.Sprintf(`Max retry attempts exceed. Client should shutdown.`))
		return Err{
			error:          kstreamErrors.Wrapf(err, `producer error retry count exceeded. Max:%d, Current:%d`, p.Producer.config.MaxRetryCount, numOfAttempts),
			shouldShutdown: true,
		}
	}
	numOfAttempts++

	p.config.Logger.Warn(p.baseProducer.GetFatalError())

	p.config.Logger.Warn(fmt.Sprintf(`Handling librd error. (Attempt %d). ErrorType:%s IsTimeout=%v, IsRetriable=%v, TxnRequiresAbort=%v, IsFatal=%v, Code=%v`,
		numOfAttempts, errorType, librdErr.IsTimeout(), librdErr.IsRetriable(), librdErr.TxnRequiresAbort(), librdErr.IsFatal(), librdErr.Code()))

	// Handle context error, after this point there is nothing to handle.
	// Note:: if a transaction is still in progress, producer Close() method will handle that
	if ctx.Err() != nil {
		p.Producer.config.Logger.Error(fmt.Sprintf(`Context already expired (ctx.Err=%v), cannot retry`, ctx.Err()))
		p.resetState()
		return Err{
			error:          kstreamErrors.Wrap(ctx.Err(), `Context already expired cannot retry`),
			shouldShutdown: true,
		}
	}

	// Check for fatal/fencing errors FIRST before any retry logic
	// These errors should never be retried as the producer is in an unrecoverable state
	if librdErr.Code() == librdKafka.ErrFenced || librdErr.Code() == librdKafka.ErrProducerFenced ||
		librdErr.Code() == librdKafka.ErrInvalidProducerIDMapping || librdErr.Code() == librdKafka.ErrFencedInstanceID ||
		librdErr.Code() == librdKafka.ErrInvalidProducerEpoch || librdErr.Code() == librdKafka.ErrOutOfOrderSequenceNumber ||
		librdErr.Code() == librdKafka.ErrUnknownProducerID || librdErr.Code() == librdKafka.ErrState ||
		librdErr.Code() == librdKafka.ErrFatal {
		p.resetState()
		return Err{
			error:   err,
			restart: true, // Note:: may lead to a loop of fencing
		}
	}

	// Check if transaction requires abort (this takes precedence over retries)
	if librdErr.TxnRequiresAbort() {
		p.config.Logger.WarnContext(ctx, fmt.Sprintf(`Transaction aborting. Reason: %s, Error %s`, errorType, err))
		p.resetState()
		return Err{
			error:       err,
			shouldAbort: true,
		}
	}

	// Now check for retriable errors
	// Note:: retriable errors for txnBegin and produce?
	if errorType == errTxInit || errorType == errTxCommit || errorType == errTxAbort || errorType == errTxSendOffsets {
		//if !librdErr.IsTimeout() && librdErr.IsRetriable() {
		if librdErr.IsRetriable() {
			if retryOp != nil {
				// Check if context is already expired - if so, retrying is pointless
				p.config.Logger.Warn(fmt.Sprintf(`Retrying transaction. Reason: %s, Error %s`, errorType, err))
				// Retry the operation and handle the result recursively
				if retryErr := retryOp(); retryErr != nil {
					return p.handleTxError(ctx, retryErr, errorType, retryOp, numOfAttempts)
				}

				return nil
			}
		}
	}

	// If the librdkafka producer queue is full, wait until some messages are flushed
	if errorType == errProduce && librdErr.Code() == librdKafka.ErrQueueFull {
		if retryOp != nil {
			p.config.Logger.Warn(fmt.Sprintf("Produce failed due to Queue full. Retrying in %s\n%s", 50*time.Millisecond,
				"If this continues, consider changing producer properties queue.buffering.max.kbytes and "+
					"queue.buffering.max.messages to a higher value",
			))
			time.Sleep(50 * time.Millisecond)
			p.config.Logger.WarnContext(ctx, fmt.Sprintf(`Retrying message. Reason: %s, Error %s`, errorType, err))
			// Retry the Warn and handle the result recursively
			if retryErr := retryOp(); retryErr != nil {
				return p.handleTxError(ctx, retryErr, errorType, retryOp, numOfAttempts)
			}
			return nil
		}

		p.resetState()
		return Err{
			error:       err,
			shouldAbort: true,
		}
	}

	p.resetState()

	// Any other errors should shut down the producer or consuming application
	return Err{
		error:          err,
		shouldShutdown: true,
	}
}

func (p *TransactionalProducer) resetState() {
	p.txBegin = false
}

// KIP-1050 aligns producer errors across client libraries, maybe we can define our errors based on that. Despite KIP-1050 is marked as complete,
// it also reports misalignment with errors defined in KIP-691.
// KIP-1050: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/309496816/KIP-1050+Consistent+error+handling+for+Transactions#KIP1050%3AConsistenterrorhandlingforTransactions-ProposedChanges
// 	- Released with 4.1.0: https://kafka.apache.org/blog/2025/09/04/apache-kafka-4.1.0-release-announcement/
// General approach for errors: https://issues.apache.org/jira/browse/KAFKA-5342

var producerFatalErrors = []librdKafka.ErrorCode{
	librdKafka.ErrFatal,
	librdKafka.ErrFenced,
	librdKafka.ErrProducerFenced,
	// New
	librdKafka.ErrTransactionalIDAuthorizationFailed,
	// New. Current library logic treats this based on librd flags. KIP-890 mentions that it can be abortable/fatal for produce requests but should
	// be fatal for txn-offset-commit requests. KIP-1050 mentions that it should be handled similarly for produce and transaction APIs.
	// To be safe, and since this is a moderately frequent error, maybe we can just treat it as fatal?
	// (However, if librd flags are reliable enough to handle this and up to date, we can keep the flag-based approach)
	// KIP-890: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/235834631/KIP-890+Transactions+Server-Side+Defense#KIP890%3ATransactionsServerSideDefense-OldClients
	// 	- Released with 4.0.0: https://kafka.apache.org/40/operations/transaction-protocol/
	// Transaction Manager (java) treats it as abortable: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java#L804
	librdKafka.ErrInvalidTxnState,
	// New. Must be fatal due to the root cause being transaction timeout configuration mismatch.
	librdKafka.ErrInvalidTransactionTimeout,
	// New. Must connect to the new coordinator. Do not need to fatal/restart if the producer silently fetches the new coordinator's metadata.
	// Java implementation does not mention its type: https://kafka.apache.org/40/javadoc/org/apache/kafka/common/errors/TransactionCoordinatorFencedException.html
	// Error: https://kafka.apache.org/43/design/protocol/#:~:text=TRANSACTION%5FCOORDINATOR%5FFENCED
	librdKafka.ErrTransactionCoordinatorFenced,
	librdKafka.ErrFencedInstanceID,
	// KIP-360 defined this as abortable (e.g. where epoch bump is possible such as idempotent producer), which was later changed to fatal again with
	// Ticket: https://issues.apache.org/jira/browse/KAFKA-18019. Already released in Java: https://github.com/apache/kafka/pull/17822
	librdKafka.ErrInvalidProducerIDMapping,
	// New. If the gapless guarantee is enabled, the producer must panic upon this error .
	// Librd config: https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html#:~:text=enable%2Egapless%2Eguarantee
	librdKafka.ErrGaplessGuarantee,
	// Errors introduced by librd:
	// librdKafka.ErrState,	// may not be surfaced to application
	// librdKafka.ErrUnsupportedFeature,
	// librdKafka.ErrCritSysResource,
	// librdKafka.ErrFs,
	// librdKafka.ErrFail,	// probably context dependent
}

var producerContextDependentErrors = []librdKafka.ErrorCode{
	// In brokers ≥2.5, abortable for produce API but fatal for transaction API. If depending on flags is not reliable, we can treat it as fatal
	// as proposed in KIP-1050. Java also suggests re-initializing the producer: https://kafka.apache.org/28/javadoc/org/apache/kafka/common/errors/InvalidProducerEpochException.html
	librdKafka.ErrInvalidProducerEpoch,
	// This can happen when the broker loses the producer's state (PID, epoch) and therefore, idempotent producer can abort, bump epoch and retry.
	// This was made from fatal to abortable in KIP-360.
	librdKafka.ErrUnknownProducerID,
	// For idempotent producer, this can be retried with the same producer. For transactional producer, this should be fatal.
	// Java implementation: https://kafka.apache.org/31/javadoc/org/apache/kafka/common/errors/OutOfOrderSequenceException.html
	librdKafka.ErrOutOfOrderSequenceNumber,
}

var producerInvalidConfigErrors = []librdKafka.ErrorCode{
	// Ticket Kafka-13668 suggests that this should not be fatal, which is already integrated to Java: https://issues.apache.org/jira/browse/KAFKA-13668
	// KIP-1050 mentions that the current behaviour can either be abortable or fatal depending on the context, but proposes to handle it in the application.
	// KIP-1050: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/309496816/KIP-1050+Consistent+error+handling+for+Transactions#KIP1050:ConsistenterrorhandlingforTransactions-Clientsidecodeexample:~:text=ClusterAuthorizationException
	librdKafka.ErrClusterAuthorizationFailed,
	// Java implementation recommends that this should generally be fatal: https://kafka.apache.org/21/javadoc/org/apache/kafka/common/errors/UnsupportedVersionException.html
	// KIP-1050 mentions the same as above.
	librdKafka.ErrUnsupportedVersion,
}
