package topology

import "github.com/gmbyapa/kstream/v2/kafka"

// ProcessorInterceptor wraps task-level record processing.
type ProcessorInterceptor interface {
	// OnProcess wraps the execution of the sub-topology for each consumed record.
	// Calling next executes the full processor chain — including all downstream operators,
	// state store writes, and produces to output topics.
	//
	// WARNING: The error returned by next MUST be returned from OnProcess. Swallowing or
	// replacing it will compromise Exactly-Once Semantics (EOS), transactional error
	// handling, and dead-letter queue (DLQ) logic.
	OnProcess(record kafka.Record, next func(kafka.Record) error) error
}

// TaskInterceptor combines processor and producer interceptors into a single
// stateful instance that shares state across both hooks.
type TaskInterceptor interface {
	ProcessorInterceptor
	kafka.ProducerInterceptor
}

// TaskInterceptorBuilder creates a task-scoped TaskInterceptor.
// It receives the task's TransactionalProducer so the interceptor can
// produce additional records (e.g., boundary markers).
type TaskInterceptorBuilder func(producer kafka.TransactionalProducer) TaskInterceptor
