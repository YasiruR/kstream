package kafka

// ConsumerInterceptor intercepts records after they are consumed from Kafka
// and before they are passed to the application.
type ConsumerInterceptor interface {
	// OnConsume is called for each record consumed from Kafka.
	// Use record.Ctx() to access the current context and record.WithCtx() to enrich it.
	OnConsume(record Record) Record
}

// ProducerInterceptor intercepts records before they are produced to Kafka.
type ProducerInterceptor interface {
	// OnProduce is called for each record before it is sent to Kafka.
	OnProduce(record Record) Record
}
