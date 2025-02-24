package started

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/models"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/transaction_outbox"
	"github.com/vysogota0399/gophermart_protos/gen/events"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type Publisher struct {
	lg     *logging.ZapLogger
	writer *kafka.Writer
}

func NewPublisher(
	lg *logging.ZapLogger,
	cfg *transaction_outbox.Config,
	globalCFG *config.Config,
	errLogger *logging.KafkaErrorLogger,
	logger *logging.KafkaLogger,
) *Publisher {
	w := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers:      globalCFG.KafkaBrokers,
			Topic:        cfg.KafkaAccrualsTopic,
			RequiredAcks: 0,
			Logger:       logger,
			ErrorLogger:  errLogger,
			Balancer:     &kafka.Hash{},
		},
	)

	return &Publisher{lg: lg, writer: w}
}

func (p *Publisher) Publish(ctx context.Context, e *models.Event) error {
	accrual := events.AccrualProcessed{
		Event: &events.AccrualProcessed_StartedEvent{
			StartedEvent: &events.StartedEvent{
				EventUuid: e.UUID,
				OrderUuid: e.Meta.OrderUUID,
			},
		},
	}

	event, err := proto.Marshal(&accrual)
	if err != nil {
		return fmt.Errorf("transaction_outbox/started/publisher: marashal failed  %w", err)
	}

	payload := kafka.Message{
		Key:   []byte(e.Meta.OrderNumber),
		Value: event,
	}

	p.lg.InfoCtx(
		ctx,
		"publish message to kafka",
		zap.String("topic", p.writer.Topic),
		zap.Any("message", e),
	)

	if err := p.writer.WriteMessages(
		ctx,
		payload,
	); err != nil {
		return fmt.Errorf("transaction_outbox/started/publisher: write message to toppic failed %w", err)
	}

	return nil
}
