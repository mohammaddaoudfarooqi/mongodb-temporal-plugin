// Package app holds the workflows and activities of the end-to-end
// example. Each workflow is shaped to drive a specific set of the
// plugin's persistence paths (see README.md); none of them contain
// business logic worth keeping.
package app

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

// TaskQueue is the task queue the example worker polls.
const TaskQueue = "mongodb-plugin-e2e"

// Custom search attributes the order workflow upserts. The runner
// registers them on the namespace before starting the worker.
const (
	SACustomerID  = "CustomerId"  // Keyword
	SAOrderTotal  = "OrderTotal"  // Double
	SAPriority    = "Priority"    // Int
	SAOrderStatus = "OrderStatus" // Keyword
)

// Order is the input of OrderWorkflow.
type Order struct {
	ID         string
	CustomerID string
	Items      []Item
	Priority   int
	// FlakyPayment makes ChargePayment fail its first two attempts so
	// the retry path is exercised.
	FlakyPayment bool
	// ApprovalThreshold: orders whose total exceeds it wait for an
	// "approve" signal for at most ApprovalTimeout.
	ApprovalThreshold float64
	ApprovalTimeout   time.Duration
	// DeclinePayment makes ChargePayment fail permanently, which drives
	// the compensation path.
	DeclinePayment bool
}

// Item is one order line.
type Item struct {
	SKU      string
	Quantity int
	Price    float64
}

// Total is the order value.
func (o Order) Total() float64 {
	var t float64
	for _, it := range o.Items {
		t += float64(it.Quantity) * it.Price
	}
	return t
}

// OrderResult is the outcome of OrderWorkflow.
type OrderResult struct {
	Status         string
	Total          float64
	TrackingNumber string
	Compensated    bool
}

// Order statuses, also upserted as the OrderStatus search attribute.
const (
	StatusReserved  = "RESERVED"
	StatusApproved  = "APPROVED"
	StatusCharged   = "CHARGED"
	StatusShipped   = "SHIPPED"
	StatusCancelled = "CANCELLED"
)

// Signal, query and update names.
const (
	SignalApprove = "approve"
	QueryStatus   = "status"
	UpdateAddItem = "add-item"
)

// Typed search-attribute keys.
var (
	CustomerIDKey  = temporal.NewSearchAttributeKeyKeyword(SACustomerID)
	OrderTotalKey  = temporal.NewSearchAttributeKeyFloat64(SAOrderTotal)
	PriorityKey    = temporal.NewSearchAttributeKeyInt64(SAPriority)
	OrderStatusKey = temporal.NewSearchAttributeKeyKeyword(SAOrderStatus)
)
