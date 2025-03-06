package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

const (
	NewState int32 = iota
	ProcessingState
	FinishedState
	FailedState
	SendState
)

type Event struct {
	UUID  string `json:"uuid"`
	Name  string `json:"event_name"`
	State int32  `json:"event_state"`
	Meta  *Meta  `json:"meta"`
}

type Meta struct {
	OrderUUID   string `json:"order_uuid"`
	OrderNumber string `json:"order_number"`
	State       int32  `json:"state,omitempty"`
	Amount      int64  `json:"amount"`
	Error       string `json:"error,omitempty"`
}

func (m *Meta) Scan(value interface{}) error {
	if value == nil {
		*m = Meta{}
		return nil
	}

	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("models/event: meta invalid format error, expected json")
	}

	return json.Unmarshal(b, &m)
}

func (m Meta) Value() (driver.Value, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("models/event meta json marshal error %w", err)
	}

	return b, nil
}
