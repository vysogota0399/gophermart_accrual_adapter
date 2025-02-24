package config

import (
	"github.com/caarlos0/env"
)

type Config struct {
	AccrualHTTPAddress    string `json:"accrual_http_address" env:"ACCRUAL_HTTP_ADDRESS" envDefault:"http://127.0.0.1:8080"`
	GophermartGRPCAddress string `json:"gophermart_grpc_address" env:"GOPHERMART_GRPC_ADDRESS" envDefault:"127.0.0.1:8050"`
	PollInterval          int    `json:"poll_inverval" env:"DAEMON_POLL_INTERVAL" envDefault:"5000"`
	WorkersCount          int64  `json:"daemon_workers_count" env:"DAEMON_WORKERS_COUNT" envDefault:"1"`

	KafkaOrderCreatedTopic                  string `json:"kafka_order_created_topic" env:"KAFKA_ORDER_CREATED_TOPIC" envDefault:"order_created"`
	KafkaOrderCreatedGroupID                string `json:"kafka_order_created_group_id" env:"KAFKA_ORDER_CREATED_GROUP_ID" envDefault:"accrual_adapter_order_created_consumer_group"`
	KafkaOrderCreatedPartitionWatchInterval int    `json:"kafka_order_created_partition_watch_interval" env:"KAFKA_ORDER_CREATED_PARTITION_WATCH_INTERWAL" envDefault:"100"`
	KafkaOrderCreatedMaxWaitInterval        int    `json:"kafka_order_created_max_wait_interval" env:"KAFKA_ORDER_CREATED_MAX_WAIT_INTERWAL" envDefault:"100"`
}

func MustNewConfig() *Config {
	c := &Config{}
	env.Parse(c)

	return c
}
