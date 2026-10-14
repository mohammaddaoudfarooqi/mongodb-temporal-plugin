package testutil

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// WaitForPrimary blocks until the server behind db reports itself as a
// writable primary. Integration tests connect with directConnection=true
// to a freshly initiated single-node replica set, which can accept
// connections a moment before it wins its election; writes issued in
// that window fail with NotWritablePrimary.
func WaitForPrimary(ctx context.Context, db *mongo.Database) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for {
		var hello struct {
			IsWritablePrimary bool `bson:"isWritablePrimary"`
		}
		err := db.Client().Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
		if err == nil && hello.IsWritablePrimary {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("testutil: replica set never became writable primary: %w (last error: %v)", ctx.Err(), err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
