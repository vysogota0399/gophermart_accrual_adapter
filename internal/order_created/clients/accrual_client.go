package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	globalCfg "github.com/vysogota0399/gophermart_accural_adapter/internal/config"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/config"
	"go.uber.org/zap"
	"resty.dev/v3"
)

type AccrualClient struct {
	lg      *logging.ZapLogger
	Address string
	client  *resty.Client
}

func NewAccrualClient(
	cfg *config.Config,
	lg *logging.ZapLogger,
	restyLogger *logging.RestyLogger,
	globalCfg *globalCfg.Config,

) *AccrualClient {
	r := resty.New()
	r.SetLogger(restyLogger)
	r.SetDebug(globalCfg.LogLevel == int(zap.DebugLevel))
	r.SetDebugLogFormatter(resty.DebugLogJSONFormatter)

	return &AccrualClient{
		Address: cfg.AccrualHTTPAddress,
		lg:      lg,
		client:  r,
	}
}

type CalculateParams struct {
	Order string                   `json:"order"`
	Goods []CalculateParamsProduct `json:"goods"`
}

type CalculateParamsProduct struct {
	Description string `json:"description"`
	Price       int64  `json:"price"`
}

func (cl *AccrualClient) Calculate(ctx context.Context, body CalculateParams) error {
	req := cl.client.R()
	req.URL = fmt.Sprintf("%s/api/orders", cl.Address)
	req.Method = http.MethodPost
	req.Body = body

	response, err := req.Send()
	if err != nil {
		return fmt.Errorf("accrual_client: calculate error %w", err)
	}

	if response.StatusCode() == http.StatusAccepted {
		return nil
	}

	return fmt.Errorf(
		"accrual_client: request failed with status %d, response: %s",
		response.StatusCode(),
		response.String(),
	)
}

type Accrual struct {
	Number string  `json:"order"`
	Amount float64 `json:"accrual"`
	Status string  `json:"status"`
}

func (a *Accrual) IsInvalid() bool {
	return a.Status == "INVALID"
}

func (a *Accrual) IsRegistered() bool {
	return a.Status == "REGISTERED"
}

func (a *Accrual) IsProcessing() bool {
	return a.Status == "PROCESSING"
}

func (a *Accrual) IsProcessed() bool {
	return a.Status == "PROCESSED"
}

type ErrSpam struct {
	Message    string
	RetryAfter time.Duration
}

func (e *ErrSpam) Error() string {
	return e.Message
}

func (cl *AccrualClient) Result(ctx context.Context, number string) (*Accrual, error) {
	req := cl.client.R()
	req.URL = fmt.Sprintf("%s/api/orders/%s", cl.Address, number)
	req.Method = http.MethodGet

	response, err := req.Send()
	if err != nil {
		return nil, fmt.Errorf("accrual_client: fetch accrual result error %w", err)
	}

	if response.StatusCode() == http.StatusTooManyRequests {
		next := response.Header().Get("Retry-After")
		nextInt, err := strconv.Atoi(next)
		if err != nil {
			return nil, fmt.Errorf("accrual_client: convert retry-after to int error %w", err)
		}

		return nil, &ErrSpam{Message: "accrual_client: no more than N requests per minute allowed", RetryAfter: time.Duration(nextInt) * time.Second}
	}

	acc := Accrual{}
	if err := json.Unmarshal([]byte(response.String()), &acc); err != nil {
		return nil, fmt.Errorf("accrual_client: parse json error %w", err)
	}
	return &acc, nil
}
