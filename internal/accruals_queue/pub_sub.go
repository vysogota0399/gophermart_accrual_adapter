package accruals_queue

import (
	"context"
	"fmt"

	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	client "github.com/vysogota0399/gophermart_accural_adapter/internal/redis"
	"github.com/vysogota0399/gophermart_protos/gen/events"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type PubSub struct {
	redis  *client.Redis
	lg     *logging.ZapLogger
	chName string
}

func NewPS(redis *client.Redis, lg *logging.ZapLogger, cfg *config.Config) *PubSub {
	return &PubSub{lg: lg, redis: redis, chName: cfg.AccrualsQueueName}
}

func (ps *PubSub) Push(ctx context.Context, e *events.OrderCreated) error {
	message, err := proto.Marshal(e)
	if err != nil {
		return fmt.Errorf("pubsub: marshal message error %w", err)
	}

	ps.lg.DebugCtx(ctx, "publish message to redis", zap.Any("message", e))
	return ps.redis.Client.Publish(ctx, ps.chName, message).Err()
}

func (ps *PubSub) Subscribe(ctx context.Context) <-chan *events.OrderCreated {
	result := make(chan *events.OrderCreated)

	go func() {
		defer close(result)

		redis := ps.redis.Client.Subscribe(ctx, ps.chName).Channel()
		for {
			select {
			case <-ctx.Done():
				ps.lg.DebugCtx(ctx, "gracefull shut down PUB/SUB")
				return
			case msg := <-redis:
				e := events.OrderCreated{}
				if err := proto.Unmarshal([]byte(msg.Payload), &e); err != nil {
					ps.lg.ErrorCtx(ctx, "proto unmarshal error!!!", zap.Error(err))
					return
				}

				ps.lg.DebugCtx(ctx, "got message from redis", zap.Any("message", &e))
				result <- &e
			}
		}
	}()

	return result
}
