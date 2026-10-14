//go:build integration

package mongo_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"

	internalconfig "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/config"
	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
)

// end to end: the documented "credential-free uri + username +
// passwordEnv" configuration must authenticate against a cluster that
// requires auth.
func TestClientProvider_URIPlusPasswordEnv_Authenticates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	container, err := mongodb.Run(ctx, testutil.MongoImage(),
		mongodb.WithReplicaSet("rs0"),
		mongodb.WithUsername("temporal"), mongodb.WithPassword("p@ss:w0rd/x"),
	)
	if err != nil {
		t.Fatalf("start mongo: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	raw, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	u.User = nil // credential-free URI, as in the README example
	q := u.Query()
	q.Del("replicaSet")
	q.Set("directConnection", "true")
	u.RawQuery = q.Encode()

	t.Setenv("TEST_MONGO_PASSWORD", "p@ss:w0rd/x")
	parsed, _, err := internalconfig.ParseConfig(map[string]any{
		"uri": u.String(), "database": "temporal_auth_test",
		"username": "temporal", "passwordEnv": "TEST_MONGO_PASSWORD", "authSource": "admin",
	})
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	provider, err := internalmongo.NewClientProvider(ctx, internalmongo.FromConfig(parsed))
	if err != nil {
		t.Fatalf("NewClientProvider should authenticate: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })

	coll := provider.Database().Collection("probe")
	if _, err := coll.InsertOne(ctx, bson.M{"_id": "x"}); err != nil {
		t.Fatalf("authenticated write failed: %v", err)
	}
}
