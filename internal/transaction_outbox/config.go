package transaction_outbox

import (
	"github.com/caarlos0/env"
)

type Config struct {
	AccrualsOutboxPollInterval int    `json:"accruals_outbox_poll_inverval" env:"ACCRUALS_OUTBOX_POLL_INTERVAL" envDefault:"10"`
	DatabaseDSN                string `json:"database_dsn" env:"DATABASE_DSN" envDefault:"postgres://postgres:secret@127.0.0.1:5432/gophermart_accrual_adapter"`
	WorkersCount               int64  `json:"workersCount" env:"DAEMON_DAEMON_WORKERS_COUNT" envDefault:"5"`
	KafkaAccrualsTopic         string `json:"kafka_accrual_topic" env:"KAFKA_ACCRUALS_TOPIC" envDefault:"accruals"`
}

func MustNewConfig() *Config {
	c := &Config{}
	env.Parse(c)

	return c
}
