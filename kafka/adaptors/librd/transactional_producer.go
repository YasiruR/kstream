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
	// Note: if a transaction is still in progress, producer Close() method will handle that
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
			restart: true,
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
