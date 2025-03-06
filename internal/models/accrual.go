package models

type Accrual struct {
	Number string  `json:"order"`
	Amount float64 `json:"accrual"`
	Status string  `json:"status"`
}

const (
	ACCRUAL_INVALID    = "INVALID"
	ACCRUAL_REGISTERED = "REGISTERED"
	ACCRUAL_PROCESSING = "PROCESSING"
	ACCRUAL_PROCESSED  = "PROCESSED"
)

func (a *Accrual) IsInvalid() bool {
	return a.Status == ACCRUAL_INVALID
}

func (a *Accrual) IsRegistered() bool {
	return a.Status == ACCRUAL_REGISTERED
}

func (a *Accrual) IsProcessing() bool {
	return a.Status == ACCRUAL_PROCESSING
}

func (a *Accrual) IsProcessed() bool {
	return a.Status == ACCRUAL_PROCESSED
}
