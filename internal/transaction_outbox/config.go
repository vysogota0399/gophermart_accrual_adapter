package transaction_outbox

import (
	"github.com/caarlos0/env"
)

type Config struct {
	AccrualsOutboxPollInterval int    `json:"accruals_outbox_poll_inverval" env:"GOPHERMART_ACCRUAL_ADAPTER_ACCRUALS_OUTBOX_POLL_INTERVAL" envDefault:"10000"`
	DatabaseDSN                string `json:"database_dsn" env:"DATABASE_DSN" envDefault:"postgres://postgres:secret@127.0.0.1:5432/gophermart_accrual_adapter"`
	WorkersCount               int64  `json:"workersCount" env:"GOPHERMART_ACCRUAL_ADAPTER_OUTBOX_DAEMON_WORKERS_COUNT" envDefault:"1"`
	KafkaAccrualsTopic         string `json:"kafka_accrual_topic" env:"KAFKA_ACCRUALS_TOPIC" envDefault:"accruals"`
}

func MustNewConfig() *Config {
	c := new(Config)
	env.Parse(c)

	return c
}
