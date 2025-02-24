package logging

import (
	"context"
	"fmt"

	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"go.uber.org/zap"
)

type KafkaLogger struct {
	ZapLogger
}

func NewKafkaLogger(cfg *config.Config) (*KafkaLogger, error) {
	baseLogger, err := NewZapLogger(&config.Config{LogLevel: cfg.KafkaLogLevel})

	if err != nil {
		return nil, err
	}

	return &KafkaLogger{
		ZapLogger: *baseLogger,
	}, nil
}

func (l *KafkaLogger) Printf(format string, a ...interface{}) {
	l.DebugCtx(
		l.WithContextFields(context.Background(), zap.String("name", "kafka")),
		fmt.Sprintf(format, a...),
	)
}

type KafkaErrorLogger struct {
	ZapLogger
}

func NewKafkaErrorLogger(cfg *config.Config) (*KafkaErrorLogger, error) {
	baseLogger, err := NewZapLogger(cfg)
	if err != nil {
		return nil, err
	}

	return &KafkaErrorLogger{
		ZapLogger: *baseLogger,
	}, nil
}

func (l *KafkaErrorLogger) Printf(format string, a ...interface{}) {
	l.ErrorCtx(
		l.WithContextFields(context.Background(), zap.String("name", "kafka")),
		fmt.Sprintf(format, a...),
	)
}
