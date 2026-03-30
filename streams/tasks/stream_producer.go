package tasks

import (
	"context"

	"github.com/gmbyapa/kstream/v2/kafka"
)

type streamProducer struct {
	kafka.TransactionalProducer
	interceptor kafka.ProducerInterceptor
}

func (p *streamProducer) ProduceAsync(ctx context.Context, record kafka.Record) error {
	if p.interceptor != nil {
		record = p.interceptor.OnProduce(record)
	}

	return p.TransactionalProducer.ProduceAsync(ctx, record)
}
