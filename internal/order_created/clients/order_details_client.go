package clients

import (
	"context"
	"fmt"

	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/config"
	"github.com/vysogota0399/gophermart_protos/gen/queries/order_details"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type OrderDetailsClient struct {
	conn       *grpc.ClientConn
	lg         *logging.ZapLogger
	gophermart order_details.QueryOrderDetailsClient
}

func NewOrderDetailsClient(lc fx.Lifecycle, cfg *config.Config, lg *logging.ZapLogger) (*OrderDetailsClient, error) {
	conn, err := grpc.NewClient(cfg.GophermartGRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	lc.Append(
		fx.StopHook(func() error {
			return conn.Close()
		}),
	)

	return &OrderDetailsClient{
		conn:       conn,
		gophermart: order_details.NewQueryOrderDetailsClient(conn),
		lg:         lg,
	}, nil
}

func (c *OrderDetailsClient) Find(ctx context.Context, number string) (*order_details.OrderDetailsResponse, error) {
	order, err := c.gophermart.OrderDetails(ctx, &order_details.OrderDetailsRequest{OrderNumber: number})
	if err != nil {
		return nil, fmt.Errorf("order_details_client: find order error %w", err)
	}

	return order, nil
}
