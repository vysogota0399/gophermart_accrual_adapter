package main

import (
	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/repositories"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/storage"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/transaction_outbox"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/transaction_outbox/failed"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/transaction_outbox/finished"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/transaction_outbox/started"
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

			// (transaction_outbox) паблишер событий начала генерации accrual
			fx.Annotate(started.NewPublisher, fx.ResultTags(`name:"started_publisher"`), fx.As(new(transaction_outbox.Publisher))),

			// (transaction_outbox) паблишер событий завершения генерации accrual
			fx.Annotate(finished.NewPublisher, fx.ResultTags(`name:"finished_publisher"`), fx.As(new(transaction_outbox.Publisher))),

			// (transaction_outbox) паблишер событий ошибки при генерации accrual
			fx.Annotate(failed.NewPublisher, fx.ResultTags(`name:"failed_publisher"`), fx.As(new(transaction_outbox.Publisher))),

			fx.Annotate(repositories.NewAccrualEventsRepository, fx.As(new(transaction_outbox.OutboxEventsRepository))),
			fx.Annotate(
				transaction_outbox.NewDaemon,
				fx.ParamTags(
					`name:"failed_publisher"`,
					`name:"finished_publisher"`,
					`name:"started_publisher"`,
				),
			),
		),
		fx.Supply(
			config.MustNewConfig(),
			transaction_outbox.MustNewConfig(),
		),
		fx.Invoke(startDaemon),
	)
}

func startDaemon(*transaction_outbox.Daemon) {}
