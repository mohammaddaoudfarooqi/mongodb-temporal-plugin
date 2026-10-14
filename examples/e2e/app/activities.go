package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// Activities are deterministic stand-ins for external services.
type Activities struct{}

// ReserveInventory pretends to reserve stock.
func (a *Activities) ReserveInventory(_ context.Context, order Order) (string, error) {
	return "res-" + order.ID, nil
}

// ReleaseInventory is the compensation of ReserveInventory.
func (a *Activities) ReleaseInventory(_ context.Context, reservationID string) error {
	return nil
}

// ChargePayment fails twice when the order asks for it (exercising
// activity retries) and fails permanently for declined payments.
func (a *Activities) ChargePayment(ctx context.Context, order Order) (string, error) {
	if order.DeclinePayment {
		return "", temporal.NewNonRetryableApplicationError("card declined", "PaymentDeclined", nil)
	}
	if order.FlakyPayment && activity.GetInfo(ctx).Attempt < 3 {
		return "", errors.New("payment gateway timeout")
	}
	return "pay-" + order.ID, nil
}

// RefundPayment is the compensation of ChargePayment.
func (a *Activities) RefundPayment(_ context.Context, paymentID string) error {
	return nil
}

// ShipOrder heartbeats a few times, like a long-running call would.
func (a *Activities) ShipOrder(ctx context.Context, order Order) (string, error) {
	for i := 0; i < 3; i++ {
		activity.RecordHeartbeat(ctx, i)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return "trk-" + order.ID, nil
}

// ProcessItem is used by the fan-out children and the big-history
// workflow.
func (a *Activities) ProcessItem(_ context.Context, n int) (int, error) {
	return n * 2, nil
}

// Cleanup runs after a cancellation.
func (a *Activities) Cleanup(_ context.Context, reason string) (string, error) {
	return "cleaned:" + reason, nil
}

// AlwaysFails fails with a non-retryable error.
func (a *Activities) AlwaysFails(_ context.Context, msg string) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf("boom: %s", msg), "Boom", nil)
}
