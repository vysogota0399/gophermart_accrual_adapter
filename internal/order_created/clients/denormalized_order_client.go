package clients

import (
	"context"
	"fmt"

	"github.com/vysogota0399/gophermart_accural_adapter/internal/logging"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/order_created/config"
	"github.com/vysogota0399/gophermart_protos/gen/services/denormalized_order"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DenormalizedOrderClient struct {
	conn       *grpc.ClientConn
	lg         *logging.ZapLogger
	gophermart denormalized_order.DenormalizedOrderServiceClient
}

func NewDenormalizedOrderClient(lc fx.Lifecycle, cfg *config.Config, lg *logging.ZapLogger) (*DenormalizedOrderClient, error) {
	conn, err := grpc.NewClient(cfg.GophermartGRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	lc.Append(
		fx.StopHook(func() error {
			return conn.Close()
		}),
	)

	return &DenormalizedOrderClient{
		conn:       conn,
		gophermart: denormalized_order.NewDenormalizedOrderServiceClient(conn),
		lg:         lg,
	}, nil
}

func (c *DenormalizedOrderClient) Find(ctx context.Context, number string) (*denormalized_order.DenormalizedOrder, error) {
	order, err := c.gophermart.OrderDetails(ctx, &denormalized_order.OrderDetailsRequest{OrderNumber: number})
	if err != nil {
		return nil, fmt.Errorf("denormalized_order_client: find order error %w", err)
	}

	return order, nil
}
