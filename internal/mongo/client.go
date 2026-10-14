package mongo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ClientConfig holds the subset of plugin Config the provider actually
// uses to build a *mongo.Client. It is intentionally a flat struct with
// no optional-pointer semantics so tests can construct it inline.
type ClientConfig struct {
	URI                      string
	Database                 string
	AppName                  string
	Username                 string
	Password                 string
	AuthSource               string
	TLSCAFile                string
	TLSCertFile              string
	TLSKeyFile               string
	TLSInsecureSkipVerify    bool
	ReadPreference           string
	WriteConcern             string
	ReadConcern              string
	RetryWrites              bool
	RetryReads               bool
	MaxPoolSize              int
	MinPoolSize              int
	MaxConnecting            int
	MaxIdleTimeMs            int
	ConnectTimeoutMs         int
	ServerSelectionTimeoutMs int
	SocketTimeoutMs          int
	HeartbeatIntervalMs      int
}

// ClientProvider holds the single MongoDB client shared across every
// store instance the factory creates. The zero value is
// valid: Close() is a no-op on it, which keeps ownership semantics tidy
// during partial test setups.
type ClientProvider struct {
	cfg    ClientConfig
	client *mongo.Client
	once   sync.Once // guards close path
}

// NewClientProvider connects to MongoDB, validates the deployment
// topology, warms up the driver with a hello/ping round-trip, and
// returns a ready-to-use provider. Blocks until warmup completes or
// ctx fires.
//
// Connect failures return an error rather than panicking, standalone
// deployments are rejected, the connection pool is private (the
// driver creates one per Client), and the appName tag is set.
func NewClientProvider(ctx context.Context, cfg ClientConfig) (*ClientProvider, error) {
	if cfg.URI == "" {
		return nil, errors.New("mongo: URI is required")
	}
	opts, err := BuildClientOptions(cfg)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongo: connect: %w", err)
	}

	// Warmup: server selection + a real round trip so the
	// first caller-triggered operation does not pay cold-start cost.
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongo: ping: %w", err)
	}

	// Topology validation: Temporal requires transactions,
	// which require a replica set or sharded cluster. After Ping the
	// driver has selected a server; inspect the description.
	if err := validateTopology(ctx, client); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}

	return &ClientProvider{cfg: cfg, client: client}, nil
}

// Client returns the shared mongo.Client. Stores use this to build
// collection handles; the provider retains ownership and Close() is
// the only way to release the connections.
func (p *ClientProvider) Client() *mongo.Client { return p.client }

// Database returns the configured Temporal database.
func (p *ClientProvider) Database() *mongo.Database {
	if p.client == nil {
		return nil
	}
	return p.client.Database(p.cfg.Database)
}

// Close disconnects the driver, budgeting 10 seconds.
// Safe on a zero-value provider.
func (p *ClientProvider) Close(ctx context.Context) error {
	var err error
	p.once.Do(func() {
		if p.client == nil {
			return
		}
		budget := 10 * time.Second
		child, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		err = p.client.Disconnect(child)
	})
	return err
}

// validateTopology pings and reads server description to ensure the
// deployment supports multi-document transactions. Single standalone
// mongod instances cannot run transactions and are rejected with a
// clear message.
//
// The driver's description package exposes topology kind; any kind
// other than Single (standalone) is acceptable — replica sets, load
// balancers, sharded clusters all support transactions.
func validateTopology(ctx context.Context, client *mongo.Client) error {
	// Ping twice: the first primed the pool; the second establishes a
	// description the driver can report on. The v2 driver's Client
	// exposes topology description only via internal APIs, so we use
	// `hello` which returns the server's view and inspect it.
	db := client.Database("admin")
	cmd := map[string]any{"hello": 1}
	res := db.RunCommand(ctx, cmd)
	if err := res.Err(); err != nil {
		return fmt.Errorf("mongo: hello: %w", err)
	}
	var reply struct {
		IsMaster       bool     `bson:"ismaster"`
		IsWritablePrim bool     `bson:"isWritablePrimary"`
		SetName        string   `bson:"setName"`
		Msg            string   `bson:"msg"` // "isdbgrid" on mongos
		Hosts          []string `bson:"hosts"`
	}
	if err := res.Decode(&reply); err != nil {
		return fmt.Errorf("mongo: decode hello: %w", err)
	}

	// Heuristic consistent with the MongoDB server topology classification:
	// - setName non-empty: replica set (or replica-set mongos)
	// - msg == "isdbgrid": sharded cluster via mongos
	// - otherwise: standalone
	if reply.SetName != "" || reply.Msg == "isdbgrid" {
		return nil
	}

	return errors.New(
		"mongo: connected to a standalone deployment, but Temporal requires a topology that supports multi-document transactions (replica set or sharded cluster)",
	)
}
