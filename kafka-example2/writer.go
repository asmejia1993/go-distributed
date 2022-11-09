package kafkaexample2

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Writer struct {
	Writer *kafka.Writer
}

func NewKafkaWriter() *Writer {
	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "user_full_info",
	}
	return &Writer{
		Writer: writer,
	}
}

func (k *Writer) WriteMessages(ctx context.Context, messages chan kafka.Message, messageCommitChan chan kafka.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-messages:
			err := k.Writer.WriteMessages(ctx, kafka.Message{
				Value: msg.Value,
			})
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
			case messageCommitChan <- msg:
			}
		}
	}
}
