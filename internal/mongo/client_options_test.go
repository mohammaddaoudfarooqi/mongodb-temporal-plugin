package mongo_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	internalconfig "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/config"
	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
)

func baseClientConfig() internalmongo.ClientConfig {
	return internalmongo.ClientConfig{
		URI: "mongodb://h1:27017/?replicaSet=rs0", Database: "temporal",
		WriteConcern: "majority", ReadConcern: "majority", ReadPreference: "primary",
		RetryWrites: true, RetryReads: true,
	}
}

func TestClientOptions_Concerns_Applied(t *testing.T) {
	cfg := baseClientConfig()
	cfg.WriteConcern = "majority"
	cfg.ReadConcern = "local"
	cfg.ReadPreference = "primaryPreferred"
	opts, err := internalmongo.BuildClientOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if opts.WriteConcern == nil || opts.WriteConcern.W != "majority" {
		t.Errorf("write concern not applied: %+v", opts.WriteConcern)
	}
	if opts.ReadConcern == nil || opts.ReadConcern.Level != "local" {
		t.Errorf("read concern not applied: %+v", opts.ReadConcern)
	}
	if opts.ReadPreference == nil || opts.ReadPreference.Mode() != readpref.PrimaryPreferredMode {
		t.Errorf("read preference not applied: %+v", opts.ReadPreference)
	}
}

// TestClientOptions_NumericWriteConcern_Applied: "1" / "2" are w:<n>.
func TestClientOptions_NumericWriteConcern_Applied(t *testing.T) {
	cfg := baseClientConfig()
	cfg.WriteConcern = "2"
	opts, err := internalmongo.BuildClientOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if opts.WriteConcern == nil || opts.WriteConcern.W != 2 {
		t.Fatalf("want w:2; got %+v", opts.WriteConcern)
	}
}

// TestClientOptions_InvalidReadPreference_Errors: typos fail startup
// instead of silently falling back to the driver default.
func TestClientOptions_InvalidReadPreference_Errors(t *testing.T) {
	cfg := baseClientConfig()
	cfg.ReadPreference = "primray"
	if _, err := internalmongo.BuildClientOptions(cfg); err == nil || !strings.Contains(err.Error(), "readPreference") {
		t.Fatalf("want readPreference error; got %v", err)
	}
}

func TestTxnOptions_SnapshotMajorityPrimary(t *testing.T) {
	o := internalmongo.TxnOptions()
	if o.ReadConcern == nil || o.ReadConcern.Level != "snapshot" {
		t.Errorf("txn read concern: %+v", o.ReadConcern)
	}
	if o.WriteConcern == nil || o.WriteConcern.W != "majority" {
		t.Errorf("txn write concern: %+v", o.WriteConcern)
	}
	if o.ReadPreference == nil || o.ReadPreference.Mode() != readpref.PrimaryMode {
		t.Errorf("txn read preference: %+v", o.ReadPreference)
	}
}

// TestClientOptions_URIWithPasswordEnv_SetsAuth: a
// credential-free URI plus username/password must authenticate.
func TestClientOptions_URIWithPasswordEnv_SetsAuth(t *testing.T) {
	cfg := baseClientConfig()
	cfg.Username, cfg.Password, cfg.AuthSource = "temporal", "s3cr3t", "admin"
	opts, err := internalmongo.BuildClientOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Auth == nil || opts.Auth.Username != "temporal" || opts.Auth.Password != "s3cr3t" || opts.Auth.AuthSource != "admin" {
		t.Fatalf("credentials not applied: %+v", opts.Auth)
	}
}

// TestClientOptions_URIUserinfo_NotOverridden: when the URI already
// carries credentials (component-built URIs), they are not replaced.
func TestClientOptions_URIUserinfo_NotOverridden(t *testing.T) {
	cfg := baseClientConfig()
	cfg.URI = "mongodb://u:p@h1:27017/?replicaSet=rs0"
	cfg.Username, cfg.Password = "u", "p"
	opts, err := internalmongo.BuildClientOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Auth == nil || opts.Auth.Username != "u" || opts.Auth.Password != "p" {
		t.Fatalf("URI credentials lost: %+v", opts.Auth)
	}
}

func TestClientOptions_MaxConnecting_Applied(t *testing.T) {
	cfg := baseClientConfig()
	cfg.MaxConnecting = 4
	opts, err := internalmongo.BuildClientOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if opts.MaxConnecting == nil || *opts.MaxConnecting != 4 {
		t.Fatalf("maxConnecting not applied: %v", opts.MaxConnecting)
	}
}

func TestClientOptions_TLSFiles_Applied(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeSelfSigned(t, dir)
	cfg := baseClientConfig()
	cfg.TLSCAFile, cfg.TLSCertFile, cfg.TLSKeyFile = certFile, certFile, keyFile
	cfg.TLSInsecureSkipVerify = true
	opts, err := internalmongo.BuildClientOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tc := opts.TLSConfig
	if tc == nil {
		t.Fatal("TLS config not applied")
	}
	if tc.RootCAs == nil || len(tc.Certificates) != 1 || !tc.InsecureSkipVerify {
		t.Fatalf("TLS config incomplete: roots=%v certs=%d insecure=%v", tc.RootCAs != nil, len(tc.Certificates), tc.InsecureSkipVerify)
	}
}

func TestClientOptions_TLSCAFileMissing_Errors(t *testing.T) {
	cfg := baseClientConfig()
	cfg.TLSCAFile = filepath.Join(t.TempDir(), "absent.pem")
	if _, err := internalmongo.BuildClientOptions(cfg); err == nil || !strings.Contains(err.Error(), "tlsCAFile") {
		t.Fatalf("want tlsCAFile error; got %v", err)
	}
}

// TestFromConfig_MapsEveryConnectionField guards against the drift that
// silently dropped TLS, maxConnecting and auth source: every ClientConfig
// field must be populated from a same-named Config field.
func TestFromConfig_MapsEveryConnectionField(t *testing.T) {
	src := &internalconfig.Config{}
	sv := reflect.ValueOf(src).Elem()
	for i := 0; i < sv.NumField(); i++ {
		f := sv.Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString("x-" + sv.Type().Field(i).Name)
		case reflect.Int:
			f.SetInt(int64(i + 1))
		case reflect.Bool:
			f.SetBool(true)
		}
	}
	got := reflect.ValueOf(internalmongo.FromConfig(src))
	for i := 0; i < got.NumField(); i++ {
		name := got.Type().Field(i).Name
		want := sv.FieldByName(name)
		if !want.IsValid() {
			t.Errorf("ClientConfig.%s has no Config counterpart", name)
			continue
		}
		if !reflect.DeepEqual(got.Field(i).Interface(), want.Interface()) {
			t.Errorf("ClientConfig.%s = %v; want %v", name, got.Field(i).Interface(), want.Interface())
		}
	}
	for _, name := range []string{"TLSCAFile", "TLSCertFile", "TLSKeyFile", "TLSInsecureSkipVerify", "MaxConnecting", "AuthSource"} {
		if _, ok := got.Type().FieldByName(name); !ok {
			t.Errorf("ClientConfig is missing %s", name)
		}
	}
}

func writeSelfSigned(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
