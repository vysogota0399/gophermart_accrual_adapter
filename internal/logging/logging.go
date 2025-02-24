package logging

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type zapFieldsKeyType string

var zapFieldsKey zapFieldsKeyType = "zapFields"

type ZapFields map[string]zap.Field

func (zf ZapFields) Append(fields ...zap.Field) ZapFields {
	zfCopy := make(ZapFields)
	for k, v := range zf {
		zfCopy[k] = v
	}

	for _, f := range fields {
		zfCopy[f.Key] = f
	}

	return zfCopy
}

type ZapLogger struct {
	logger *zap.Logger
	level  zap.AtomicLevel
}

func NewZapLogger(cfg *config.Config) (*ZapLogger, error) {
	fmt.Printf("Construct logger - got config %+v", cfg)
	atomic := zap.NewAtomicLevelAt(zapcore.Level(cfg.LogLevel))
	settings := defaultSettings(atomic)

	l, err := settings.config.Build(settings.opts...)
	if err != nil {
		return nil, err
	}

	return &ZapLogger{
		logger: l,
		level:  atomic,
	}, nil
}

func (z *ZapLogger) WithContextFields(ctx context.Context, fields ...zap.Field) context.Context {
	ctxFields, _ := ctx.Value(zapFieldsKey).(ZapFields)
	if ctxFields == nil {
		ctxFields = make(ZapFields)
	}
	merged := ctxFields.Append(fields...)

	return context.WithValue(ctx, zapFieldsKey, merged)
}

func (z *ZapLogger) maskField(f zap.Field) zap.Field {
	if f.Key == "password" {
		return zap.String(f.Key, "******")
	}

	if f.Key == "email" {
		email := f.String
		parts := strings.Split(email, "@")
		if len(parts) == 2 {
			return zap.String(f.Key, "***@"+parts[1])
		}
	}
	return f
}

func (z *ZapLogger) Sync() {
	_ = z.logger.Sync()
}

func (z *ZapLogger) withCtxFields(ctx context.Context, fields ...zap.Field) []zap.Field {
	var mrgdFields ZapFields
	ctxFields, ok := ctx.Value(zapFieldsKey).(ZapFields)
	if ok {
		mrgdFields = ctxFields.Append(fields...)
	} else {
		ctxFields = make(ZapFields)
		mrgdFields = ctxFields.Append(fields...)
	}

	maskedFields := make([]zap.Field, 0, len(mrgdFields))

	for _, f := range mrgdFields {
		maskedFields = append(maskedFields, z.maskField(f))
	}

	return maskedFields
}

func (z *ZapLogger) InfoCtx(ctx context.Context, msg string, fields ...zap.Field) {
	z.logger.Info(msg, z.withCtxFields(ctx, fields...)...)
}

func (z *ZapLogger) DebugCtx(ctx context.Context, msg string, fields ...zap.Field) {
	z.logger.Debug(msg, z.withCtxFields(ctx, fields...)...)
}

func (z *ZapLogger) WarnCtx(ctx context.Context, msg string, fields ...zap.Field) {
	z.logger.Warn(msg, z.withCtxFields(ctx, fields...)...)
}

func (z *ZapLogger) ErrorCtx(ctx context.Context, msg string, fields ...zap.Field) {
	zfields := z.withCtxFields(ctx, fields...)
	z.logger.Error(msg, zfields...)
}

func (z *ZapLogger) FatalCtx(ctx context.Context, msg string, fields ...zap.Field) {
	z.logger.Fatal(msg, z.withCtxFields(ctx, fields...)...)
}

func (z *ZapLogger) PanicCtx(ctx context.Context, msg string, fields ...zap.Field) {
	z.logger.Panic(msg, z.withCtxFields(ctx, fields...)...)
}

func (z *ZapLogger) SetLevel(level zapcore.Level) {
	z.level.SetLevel(level)
}

func (z *ZapLogger) Std() *log.Logger {
	return zap.NewStdLog(z.logger)
}
