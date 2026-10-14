//go:build integration

// MongoDB replica-set driver for the conformance suite. Exercises the
// whole suite against a testcontainer-managed replica set whose image
// is selected by MONGO_VERSION (default mongo:6). The CI
// integration-matrix job flips MONGO_VERSION to iterate the 6.0 /
// 7.0 / 8.0 matrix using exactly the same test binary.
package conformance_test

import (
	"context"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/testutil/conformance"
)

// mongoFixture spins one replica-set container per top-level test and
// hands each subtest a per-test database name so scenarios don't
// collide. Container cleanup is owned by the top-level t.Cleanup.
type mongoFixture struct {
	client *mongo.Client
	dbBase string
	seq    atomic.Int64
}

func startMongoFixture(t *testing.T) *mongoFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := mongodb.Run(ctx, testutil.MongoImage(), mongodb.WithReplicaSet("rs0"))
	if err != nil {
		t.Fatalf("start mongo replica set: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	rawURI, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	u, _ := url.Parse(rawURI)
	q := u.Query()
	q.Del("replicaSet")
	u.RawQuery = q.Encode()
	uri := strings.TrimRight(u.String(), "?") + "?directConnection=true"

	provider, err := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI: uri, Database: "conformance_base", AppName: "conformance",
	})
	if err != nil {
		t.Fatalf("NewClientProvider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close(context.Background()) })

	return &mongoFixture{
		client: provider.Database().Client(),
		dbBase: "conformance",
	}
}

func (f *mongoFixture) Capabilities() conformance.Capabilities {
	return conformance.Capabilities{
		Transactions: true,
		// Atlas Search requires an Atlas cluster; testcontainer replica
		// sets don't have it. Driver_atlas handles that capability.
		AtlasSearch: false,
		Sharded:     false,
	}
}

// DatabaseForSubtest returns a per-test database so parallel subtests
// don't collide on collection names.
func (f *mongoFixture) DatabaseForSubtest(t *testing.T) *mongo.Database {
	t.Helper()
	n := f.seq.Add(1)
	db := f.client.Database(f.dbBase + "_" + sanitizeName(t.Name()) + "_" + itoaLite(n))
	t.Cleanup(func() {
		_ = db.Drop(context.Background())
	})
	return db
}

func (f *mongoFixture) AtlasSearchIndexName() string                       { return "" }
func (f *mongoFixture) EnsureAtlasSearchIndex(*testing.T, *mongo.Database) {}

// TestConformance_Mongo drives the entire suite against whatever
// Mongo version MONGO_VERSION selects. CI sets this env for each
// version in the matrix and runs this one test.
func TestConformance_Mongo(t *testing.T) {
	f := startMongoFixture(t)
	conformance.RunAll(t, f)
}

// sanitizeName strips characters MongoDB doesn't allow in database
// names. Subtest names include `/` and `[` etc. — replace with `_`.
func sanitizeName(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			out = append(out, c)
		} else {
			out = append(out, '_')
		}
	}
	if len(out) > 40 {
		out = out[:40]
	}
	return string(out)
}

func itoaLite(n int64) string {
	if n == 0 {
		return "0"
	}
	buf := []byte{}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}
