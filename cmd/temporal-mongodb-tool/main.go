// Command temporal-mongodb-tool is the schema CLI for the plugin.
// Subcommands:
//
//	setup-schema        — apply collection + index manifest
//	update-schema       — apply schema migrations (state machine)
//	verify              — report current schema version + state
//	setup-atlas-search  — apply Atlas Search index manifest
//	drop-atlas-search   — remove Atlas Search indexes
//
// Usage:
//
//	temporal-mongodb-tool --uri <mongo-uri> --database <db> <subcommand> [flags]
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	internalmongo "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/mongo"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/schema"
	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/visibility"
)

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stdout)
		return
	}
	sub := os.Args[1]
	args := os.Args[2:]
	switch sub {
	case "setup-schema":
		runSetupSchema(args)
	case "update-schema":
		runUpdateSchema(args)
	case "verify":
		runVerify(args)
	case "setup-atlas-search":
		runSetupAtlasSearch(args)
	case "drop-atlas-search":
		runDropAtlasSearch(args)
	case "-h", "--help", "help":
		printUsage(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "temporal-mongodb-tool: unknown subcommand %q\n", sub)
		printUsage(os.Stderr)
		os.Exit(2)
	}
}

type sharedFlags struct {
	URI              string
	Database         string
	SchemaDir        string
	AtlasSchemaDir   string
	SchemaVersion    string
	NumHistoryShards int
	AtlasIndexName   string
	TimeoutSeconds   int
}

func bindShared(fs *flag.FlagSet, sf *sharedFlags) {
	fs.StringVar(&sf.URI, "uri", "", "MongoDB connection URI")
	fs.StringVar(&sf.Database, "database", "", "Database name")
	fs.StringVar(&sf.SchemaDir, "schema-dir", "./schema/versioned",
		"Path to the schema/versioned/ root")
	fs.StringVar(&sf.AtlasSchemaDir, "atlas-schema-dir", "./schema/atlas_search",
		"Path to the Atlas Search manifest directory")
	fs.StringVar(&sf.SchemaVersion, "schema-version", "",
		"Schema version directory name to apply (e.g. v1.1; default: the latest under --schema-dir)")
	fs.IntVar(&sf.NumHistoryShards, "num-history-shards", 0,
		"numHistoryShards to record in _schema_version on first init")
	fs.StringVar(&sf.AtlasIndexName, "atlas-index-name", "visibility_executions_search",
		"Atlas Search index name to create/drop")
	fs.IntVar(&sf.TimeoutSeconds, "timeout", 600,
		"Operation timeout in seconds; defaults to 10 minutes")
}

// bindStores registers --stores: which stores the database holds. Both
// stores share one database by default; with separate databases, pass
// core for the persistence database and visibility for the visibility
// database, so neither gets the other's collections.
func bindStores(fs *flag.FlagSet) *string {
	return fs.String("stores", "all",
		"Stores this database holds: all (one database for both), core or visibility")
}

// loadManifests reads a version's manifests, keeping only what a
// database holding stores needs. It refuses a single store on a database
// whose other store already has data: _schema_version is per database,
// so upgrading one store there would mark the other one done too.
func loadManifests(ctx context.Context, db *mongo.Database, versionDir string, stores schema.Stores) (*schema.Collections, *schema.Indexes) {
	collections, err := schema.LoadCollections(versionDir + "/collections.json")
	if err != nil {
		log.Fatal(err)
	}
	indexes, err := schema.LoadIndexes(versionDir + "/indexes.json")
	if err != nil {
		log.Fatal(err)
	}
	if stores != schema.StoresAll {
		counts := map[string]int64{}
		for _, c := range collections.Collections {
			n, err := db.Collection(c.Name).EstimatedDocumentCount(ctx)
			if err != nil {
				log.Fatal(err)
			}
			counts[c.Name] = n
		}
		if other := schema.OtherStoreData(collections, stores, counts); len(other) > 0 {
			log.Fatalf("--stores %s: database %s also holds the other store's data (%s); use --stores all",
				stores, db.Name(), strings.Join(other, ", "))
		}
	}
	collections = collections.ForStores(stores)
	return collections, indexes.ForCollections(collections)
}

// versionDir resolves the schema version directory, defaulting to the
// newest one under --schema-dir.
func (sf *sharedFlags) versionDir() string {
	if sf.SchemaVersion == "" {
		latest, err := schema.LatestVersionIn(sf.SchemaDir)
		if err != nil {
			log.Fatal(err)
		}
		sf.SchemaVersion = latest
	}
	return sf.SchemaDir + "/" + sf.SchemaVersion
}

func openProvider(ctx context.Context, sf *sharedFlags) *internalmongo.ClientProvider {
	if sf.URI == "" || sf.Database == "" {
		log.Fatal("temporal-mongodb-tool: --uri and --database are required")
	}
	p, err := internalmongo.NewClientProvider(ctx, internalmongo.ClientConfig{
		URI:          sf.URI,
		Database:     sf.Database,
		AppName:      "temporal-mongodb-tool",
		WriteConcern: "majority",
		ReadConcern:  "majority",
		RetryWrites:  true,
		RetryReads:   true,
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	return p
}

// --- setup-schema ---

func runSetupSchema(args []string) {
	var sf sharedFlags
	fs := flag.NewFlagSet("setup-schema", flag.ExitOnError)
	bindShared(fs, &sf)
	storesFlag := bindStores(fs)
	_ = fs.Parse(args)
	stores, err := schema.ParseStores(*storesFlag)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(sf.TimeoutSeconds)*time.Second)
	defer cancel()

	p := openProvider(ctx, &sf)
	defer p.Close(context.Background())
	db := p.Database()

	// schema-dir points at schema/versioned; VersionedDir expects a
	// repo root that contains schema/versioned/<ver>/. Pass "" for the
	// root and build the path from --schema-dir directly.
	versionDir := sf.versionDir()
	collections, indexes := loadManifests(ctx, db, versionDir, stores)

	created, err := schema.ApplyCollections(ctx, db, collections)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("collections: %d created, %d already present\n",
		len(created), len(collections.Collections)-len(created))

	res, err := schema.ApplyIndexes(ctx, db, indexes)
	if err != nil {
		log.Fatalf("apply indexes: %v", err)
	}
	fmt.Printf("indexes: %d created, %d already-matching, %d mismatched\n",
		len(res.IndexesCreated), len(res.IndexesSkipped), len(res.IndexesMismatched))

	meta, err := schema.ReadMetadata(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	if meta == nil {
		if sf.NumHistoryShards <= 0 {
			log.Println("warn: --num-history-shards not set; _schema_version.num_history_shards=0")
		}
		if err := schema.WriteInitialMetadata(ctx, db, collections.SchemaVersion, int32(sf.NumHistoryShards)); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("_schema_version: initialised at %s (num_history_shards=%d)\n",
			collections.SchemaVersion, sf.NumHistoryShards)
	} else {
		fmt.Printf("_schema_version: already present (%s, state=%s)\n",
			meta.SchemaVersion, meta.State)
	}
}

// --- update-schema ---

func runUpdateSchema(args []string) {
	var sf sharedFlags
	fs := flag.NewFlagSet("update-schema", flag.ExitOnError)
	bindShared(fs, &sf)
	storesFlag := bindStores(fs)
	_ = fs.Parse(args)
	stores, err := schema.ParseStores(*storesFlag)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(sf.TimeoutSeconds)*time.Second)
	defer cancel()

	p := openProvider(ctx, &sf)
	defer p.Close(context.Background())
	db := p.Database()

	meta, err := schema.ReadMetadata(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	if meta == nil {
		log.Fatal("update-schema: _schema_version absent; run setup-schema first")
	}

	versionDir := sf.versionDir()
	collections, indexes := loadManifests(ctx, db, versionDir, stores)

	// Upgrade refuses downgrades, creates missing collections, replaces
	// indexes whose definition changed, and records the new version
	//.
	from := meta.SchemaVersion
	res, err := schema.Upgrade(ctx, db, collections, indexes)
	if err != nil {
		log.Fatalf("update-schema: %v", err)
	}
	if from == indexes.SchemaVersion {
		fmt.Printf("update-schema: already at %s; nothing to do\n", from)
		return
	}
	fmt.Printf("collections: %d created; indexes: %d created, %d already-matching, %d replaced\n",
		len(res.CollectionsCreated), len(res.IndexesCreated), len(res.IndexesSkipped), len(res.IndexesMismatched))
	fmt.Printf("update-schema: %s → %s (ready)\n", from, indexes.SchemaVersion)
}

// --- verify ---

func runVerify(args []string) {
	var sf sharedFlags
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	bindShared(fs, &sf)
	_ = fs.Parse(args)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(sf.TimeoutSeconds)*time.Second)
	defer cancel()
	p := openProvider(ctx, &sf)
	defer p.Close(context.Background())

	meta, err := schema.ReadMetadata(ctx, p.Database())
	if err != nil {
		log.Fatal(err)
	}
	if meta == nil {
		fmt.Println("verify: _schema_version absent — cluster not yet initialised")
		os.Exit(1)
	}
	payload, _ := json.MarshalIndent(meta, "", "  ")
	fmt.Println(string(payload))
	if meta.State != string(schema.StateReady) {
		fmt.Fprintf(os.Stderr, "verify: WARNING state=%q (not ready)\n", meta.State)
		os.Exit(2)
	}
}

// --- setup-atlas-search / drop-atlas-search ---

type atlasManifest struct {
	Name       string         `json:"name"`
	Collection string         `json:"collection"`
	Definition map[string]any `json:"definition"`
}

func runSetupAtlasSearch(args []string) {
	var sf sharedFlags
	fs := flag.NewFlagSet("setup-atlas-search", flag.ExitOnError)
	bindShared(fs, &sf)
	_ = fs.Parse(args)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(sf.TimeoutSeconds)*time.Second)
	defer cancel()
	p := openProvider(ctx, &sf)
	defer p.Close(context.Background())
	db := p.Database()

	raw, err := os.ReadFile(sf.AtlasSchemaDir + "/visibility.json")
	if err != nil {
		log.Fatalf("read manifest: %v", err)
	}
	var m atlasManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		log.Fatalf("parse manifest: %v", err)
	}
	if sf.AtlasIndexName != "" {
		m.Name = sf.AtlasIndexName
	}

	// Atlas requires the target collection to exist before
	// createSearchIndexes accepts it. Create-if-missing; tolerate
	// race with concurrent setup-schema runs.
	if err := db.CreateCollection(ctx, m.Collection); err != nil {
		if !strings.Contains(err.Error(), "already exists") &&
			!strings.Contains(err.Error(), "NamespaceExists") {
			log.Fatalf("ensure collection %s: %v", m.Collection, err)
		}
	}

	st, err := visibility.QueryIndexStatus(ctx, db, m.Collection, m.Name)
	if err != nil {
		log.Fatalf("QueryIndexStatus: %v", err)
	}
	if st == visibility.IndexStatusUnknown {
		cmd := bson.D{
			{Key: "createSearchIndexes", Value: m.Collection},
			{Key: "indexes", Value: []bson.M{{"name": m.Name, "definition": m.Definition}}},
		}
		if err := db.RunCommand(ctx, cmd).Err(); err != nil {
			log.Fatalf("createSearchIndexes: %v", err)
		}
		fmt.Printf("atlas-search: created index %s\n", m.Name)
	} else {
		cmd := bson.D{
			{Key: "updateSearchIndex", Value: m.Collection},
			{Key: "name", Value: m.Name},
			{Key: "definition", Value: m.Definition},
		}
		if err := db.RunCommand(ctx, cmd).Err(); err != nil {
			log.Printf("updateSearchIndex (continuing): %v", err)
		} else {
			fmt.Printf("atlas-search: refreshed index %s (was %s)\n", m.Name, st)
		}
	}

	// Poll until READY.
	deadline := time.Now().Add(time.Duration(sf.TimeoutSeconds) * time.Second)
	lastLog := time.Time{}
	for time.Now().Before(deadline) {
		st, err := visibility.QueryIndexStatus(ctx, db, m.Collection, m.Name)
		if err != nil {
			log.Printf("QueryIndexStatus transient: %v", err)
		}
		if st == visibility.IndexStatusReady {
			fmt.Printf("atlas-search: %s is READY\n", m.Name)
			return
		}
		if time.Since(lastLog) > 10*time.Second {
			fmt.Printf("atlas-search: %s status=%s — waiting…\n", m.Name, st)
			lastLog = time.Now()
		}
		time.Sleep(5 * time.Second)
	}
	log.Fatalf("atlas-search: %s did not reach READY within --timeout=%ds",
		m.Name, sf.TimeoutSeconds)
}

func runDropAtlasSearch(args []string) {
	var sf sharedFlags
	fs := flag.NewFlagSet("drop-atlas-search", flag.ExitOnError)
	bindShared(fs, &sf)
	_ = fs.Parse(args)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(sf.TimeoutSeconds)*time.Second)
	defer cancel()
	p := openProvider(ctx, &sf)
	defer p.Close(context.Background())
	db := p.Database()

	cmd := bson.D{
		{Key: "dropSearchIndex", Value: "visibility_executions"},
		{Key: "name", Value: sf.AtlasIndexName},
	}
	if err := db.RunCommand(ctx, cmd).Err(); err != nil {
		if errors.Is(err, os.ErrNotExist) ||
			strings.Contains(err.Error(), "not found") ||
			strings.Contains(err.Error(), "IndexNotFound") {
			fmt.Printf("atlas-search: %s already absent\n", sf.AtlasIndexName)
			return
		}
		log.Fatalf("dropSearchIndex: %v", err)
	}
	fmt.Printf("atlas-search: dropped index %s\n", sf.AtlasIndexName)
}

func printUsage(w *os.File) {
	fmt.Fprintln(w, "temporal-mongodb-tool — schema utility for mongodb-temporal-plugin")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Subcommands:")
	fmt.Fprintln(w, "  setup-schema        apply collection + index manifest to a cluster")
	fmt.Fprintln(w, "  update-schema       apply schema migrations to a cluster")
	fmt.Fprintln(w, "  setup-atlas-search  create Atlas Search indexes (M30+)")
	fmt.Fprintln(w, "  drop-atlas-search   remove Atlas Search indexes")
	fmt.Fprintln(w, "  verify              report schema version + state")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Common flags:")
	fmt.Fprintln(w, "  --uri              MongoDB connection URI (required)")
	fmt.Fprintln(w, "  --database         Database name (required)")
	fmt.Fprintln(w, "  --schema-dir       Path to schema/versioned/ (default ./schema/versioned)")
	fmt.Fprintln(w, "  --atlas-schema-dir Path to schema/atlas_search/ (default ./schema/atlas_search)")
	fmt.Fprintln(w, "  --schema-version   Version directory to apply (default: latest)")
	fmt.Fprintln(w, "  --timeout          Seconds (default 600)")
}
