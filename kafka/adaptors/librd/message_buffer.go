/*
 * MIT License
 *
 * Copyright (c) 2023 Gayan Yapa
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package librd

//import (
//	"fmt"
//	librd "github.com/confluentinc/confluent-kafka-go/v2/kafka"
//	"github.com/gmbyapa/kstream/v2/kafka"
//	"time"
//)
//
//type MessageBuffer struct {
//	messages chan kafka.Record
//	consumer librd.Consumer
//
//	stopping       chan stopSignal
//}
//
//func NewMessageBuffer() *MessageBuffer {
//	return &MessageBuffer{
//		messages: make(chan kafka.Record),
//	}
//}
//
//func (mb *MessageBuffer) Messages() <-chan kafka.Record  {
//
//}
//
//func (mb *MessageBuffer) Start()   {
//	var err error
//MAIN:
//	for {
//		select {
//		case <-mb.stopping:
//			mb.config.Logger.Info(`Stopping consumer loop due to stop signal`)
//			break MAIN
//		default:
//			ev := mb.consumer.Poll(int(mb.config.MaxPollInterval.Milliseconds()))
//			if ev == nil {
//				continue
//			}
//
//			switch e := ev.(type) {
//			case *librdKafka.Message:
//				t := time.Since(e.Timestamp)
//
//				record := &Record{librd: e}
//				record.ctx = context.Background()
//				if mb.config.ContextExtractor != nil {
//					record.ctx = mb.config.ContextExtractor(record)
//				}
//
//				mb.config.Logger.DebugContext(record.ctx, fmt.Sprintf(`Message %s with key (%s) received in %s`,
//					record, record.Key(), t))
//
//				mb.metrics.endToEndLatency.Observe(float64(t), map[string]string{
//					`topic_partition`: fmt.Sprintf(`%s_%d`, record.Topic(), record.Partition()),
//				})
//
//				mb.messageChan <- record
//
//			case librdKafka.PartitionEOF:
//				mb.config.Logger.Info(fmt.Sprintf(`Partition end %s`, e))
//
//			case librdKafka.Error:
//				mb.config.Logger.Warn(fmt.Sprintf(`Consume error due to %s`, e))
//			default:
//			}
//		}
//	}
//
//	mb.config.Logger.Info(`Consumer closing...`)
//	defer mb.config.Logger.Info(`Consumer closed`)
//
//	if err := mb.consumer.Close(); err != nil {
//		return errors.Wrapf(err, `consumer closer error. ConsumerID: %s`, mb.config.Id)
//	}
//
//	return err
//}
//
//func (mb *MessageBuffer) Drain()  {
//	close(mb.messages)
//	for  range mb.messages {}
//}
//
