package mongo

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	internalconfig "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/config"
)

// FromConfig projects the parsed plugin configuration onto the subset the
// client provider needs. Both the data-store and visibility factories use
// it so no connection option can be dropped by one of them.
func FromConfig(c *internalconfig.Config) ClientConfig {
	return ClientConfig{
		URI:                      c.URI,
		Database:                 c.Database,
		AppName:                  c.AppName,
		Username:                 c.Username,
		Password:                 c.Password,
		AuthSource:               c.AuthSource,
		TLSCAFile:                c.TLSCAFile,
		TLSCertFile:              c.TLSCertFile,
		TLSKeyFile:               c.TLSKeyFile,
		TLSInsecureSkipVerify:    c.TLSInsecureSkipVerify,
		ReadPreference:           c.ReadPreference,
		WriteConcern:             c.WriteConcern,
		ReadConcern:              c.ReadConcern,
		RetryWrites:              c.RetryWrites,
		RetryReads:               c.RetryReads,
		MaxPoolSize:              c.MaxPoolSize,
		MinPoolSize:              c.MinPoolSize,
		MaxConnecting:            c.MaxConnecting,
		MaxIdleTimeMs:            c.MaxIdleTimeMs,
		ConnectTimeoutMs:         c.ConnectTimeoutMs,
		ServerSelectionTimeoutMs: c.ServerSelectionTimeoutMs,
		SocketTimeoutMs:          c.SocketTimeoutMs,
		HeartbeatIntervalMs:      c.HeartbeatIntervalMs,
	}
}

// BuildClientOptions turns a ClientConfig into driver options. Explicit
// settings win over anything carried in the URI.
func BuildClientOptions(cfg ClientConfig) (*options.ClientOptions, error) {
	appName := cfg.AppName
	if appName == "" {
		appName = "temporal-mongodb-plugin"
	}

	opts := options.Client().
		ApplyURI(cfg.URI).
		SetAppName(appName).
		SetRetryWrites(cfg.RetryWrites).
		SetRetryReads(cfg.RetryReads)

	if cfg.WriteConcern != "" {
		opts.SetWriteConcern(parseWriteConcern(cfg.WriteConcern))
	}
	if cfg.ReadConcern != "" {
		opts.SetReadConcern(&readconcern.ReadConcern{Level: cfg.ReadConcern})
	}
	if cfg.ReadPreference != "" {
		mode, err := readpref.ModeFromString(cfg.ReadPreference)
		if err != nil {
			return nil, fmt.Errorf("mongo: readPreference %q: %w", cfg.ReadPreference, err)
		}
		rp, err := readpref.New(mode)
		if err != nil {
			return nil, fmt.Errorf("mongo: readPreference %q: %w", cfg.ReadPreference, err)
		}
		opts.SetReadPreference(rp)
	}

	// Credentials embedded in the URI (including URIs synthesised from
	// host components) take precedence; otherwise apply the separately
	// configured username/password, e.g. a bare URI plus passwordEnv.
	if opts.Auth == nil && cfg.Username != "" {
		opts.SetAuth(options.Credential{
			Username:   cfg.Username,
			Password:   cfg.Password,
			AuthSource: cfg.AuthSource,
		})
	}

	tlsCfg, err := buildTLSConfig(cfg, opts.TLSConfig)
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		opts.SetTLSConfig(tlsCfg)
	}

	if cfg.MaxPoolSize > 0 {
		opts.SetMaxPoolSize(uint64(cfg.MaxPoolSize))
	}
	if cfg.MinPoolSize > 0 {
		opts.SetMinPoolSize(uint64(cfg.MinPoolSize))
	}
	if cfg.MaxConnecting > 0 {
		opts.SetMaxConnecting(uint64(cfg.MaxConnecting))
	}
	idleMs := cfg.MaxIdleTimeMs
	if idleMs <= 0 {
		idleMs = 30_000
	}
	opts.SetMaxConnIdleTime(time.Duration(idleMs) * time.Millisecond)
	if cfg.ConnectTimeoutMs > 0 {
		opts.SetConnectTimeout(time.Duration(cfg.ConnectTimeoutMs) * time.Millisecond)
	}
	if cfg.ServerSelectionTimeoutMs > 0 {
		opts.SetServerSelectionTimeout(time.Duration(cfg.ServerSelectionTimeoutMs) * time.Millisecond)
	}
	if cfg.SocketTimeoutMs > 0 {
		// v2 driver replaced SocketTimeout with per-op timeouts; we
		// apply it as a default timeout here so caller contexts still
		// win where narrower.
		opts.SetTimeout(time.Duration(cfg.SocketTimeoutMs) * time.Millisecond)
	}
	if cfg.HeartbeatIntervalMs > 0 {
		opts.SetHeartbeatInterval(time.Duration(cfg.HeartbeatIntervalMs) * time.Millisecond)
	}
	return opts, nil
}

// TxnOptions are applied to every multi-document transaction: snapshot
// reads so the shard fence and CAS reads see one consistent view, and
// majority writes so a committed transaction survives failover.
func TxnOptions() *options.TransactionOptions {
	return &options.TransactionOptions{
		ReadConcern:    readconcern.Snapshot(),
		WriteConcern:   writeconcern.Majority(),
		ReadPreference: readpref.Primary(),
	}
}

// txnOptionsLister adapts TxnOptions to the driver's Lister interface.
func txnOptionsLister() options.Lister[options.TransactionOptions] {
	o := TxnOptions()
	return options.Transaction().
		SetReadConcern(o.ReadConcern).
		SetWriteConcern(o.WriteConcern).
		SetReadPreference(o.ReadPreference)
}

// parseWriteConcern accepts "majority", a node count ("1", "2"), or a
// custom tag-set name.
func parseWriteConcern(w string) *writeconcern.WriteConcern {
	if w == "majority" {
		return writeconcern.Majority()
	}
	if n, err := strconv.Atoi(w); err == nil {
		return &writeconcern.WriteConcern{W: n}
	}
	return &writeconcern.WriteConcern{W: w}
}

// buildTLSConfig layers the configured TLS files on top of any TLS
// settings the URI already enabled. Returns nil when TLS is not
// configured at all.
func buildTLSConfig(cfg ClientConfig, fromURI *tls.Config) (*tls.Config, error) {
	if cfg.TLSCAFile == "" && cfg.TLSCertFile == "" && cfg.TLSKeyFile == "" && !cfg.TLSInsecureSkipVerify {
		return nil, nil
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if fromURI != nil {
		tc = fromURI.Clone()
	}
	if cfg.TLSCAFile != "" {
		pem, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("mongo: tlsCAFile: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("mongo: tlsCAFile %q contains no PEM certificates", cfg.TLSCAFile)
		}
		tc.RootCAs = pool
	}
	if cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" {
		keyFile := cfg.TLSKeyFile
		if keyFile == "" {
			// A combined PEM holding both certificate and key is common
			// for MongoDB client certificates.
			keyFile = cfg.TLSCertFile
		}
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("mongo: tlsCertFile/tlsKeyFile: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	if cfg.TLSInsecureSkipVerify {
		tc.InsecureSkipVerify = true //nolint:gosec // operator opt-in, documented as test-only
	}
	return tc, nil
}
