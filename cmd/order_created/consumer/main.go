package main

import (
	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	order_created_config "github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/config"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/consumer"
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

			consumer.NewConsumer,
			fx.Annotate(repositories.NewOrderEventsRepository, fx.As(new(consumer.ConsumerInboxEventsRepository))),
			fx.Annotate(repositories.NewAccrualEventsRepository, fx.As(new(repositories.AccrualEvents))),
		),
		fx.Supply(
			config.MustNewConfig(),
			order_created_config.MustNewConfig(),
		),
		fx.Invoke(

			startConsumer,
		),
	)
}
func startConsumer(*consumer.Consumer) {}
