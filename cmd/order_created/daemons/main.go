package main

import (
	"github.com/vysogota0399/gophermart_accural_adapter/internal/accruals_queue"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/accruals_processor"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/clients"
	order_created_config "github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/config"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/events_processor"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/redis"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/repositories"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/storage"
	"go.uber.org/fx"
)

func main() {
	fx.New(CreateApp()).Run()
}

func CreateApp() fx.Option {
	return fx.Options(
		fx.Provide(
			storage.NewStorage,
			logging.NewZapLogger,
			logging.NewKafkaErrorLogger,
			logging.NewKafkaLogger,
			logging.NewRestyLogger,
			redis.NewRedis,

			// (events_processor) обработчик событий создания заказа
			events_processor.NewDaemon,
			fx.Annotate(repositories.NewOrderEventsRepository, fx.As(new(events_processor.EventsRepository))),
			fx.Annotate(accruals_queue.NewPS, fx.As(new(events_processor.AccrualsQueue))),
			fx.Annotate(clients.NewDenormalizedOrderClient, fx.As(new(events_processor.OrderClient))),
			fx.Annotate(clients.NewAccrualClient, fx.As(new(events_processor.AccrualClient))),
			fx.Annotate(repositories.NewAccrualEventsRepository, fx.As(new(repositories.AccrualEvents))),

			// (events_processor) обработчик очереди расчетов
			accruals_processor.NewDaemon,
			fx.Annotate(accruals_queue.NewPS, fx.As(new(accruals_processor.AccrualsQueue))),
			fx.Annotate(clients.NewAccrualClient, fx.As(new(accruals_processor.AccrualClient))),
			fx.Annotate(repositories.NewAccrualEventsRepository, fx.As(new(accruals_processor.AccrualEvents))),
		),
		fx.Supply(
			config.MustNewConfig(),
			order_created_config.MustNewConfig(),
		),
		fx.Invoke(
			startEventsProcessor,
			startAccrualProcessor,
		),
	)
}

func startEventsProcessor(*events_processor.Daemon)    {}
func startAccrualProcessor(*accruals_processor.Daemon) {}
