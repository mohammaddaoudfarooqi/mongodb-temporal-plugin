// Command temporal-mongodb-server is a Temporal server binary that wires
// the MongoDB persistence + visibility plugin into `temporal.NewServer`.
//
// At M0 the binary only demonstrates plugin wiring compiles — it does not
// own its own CLI flag set, nor does it run the Temporal server by
// default. When invoked with `--start`, it attempts to build and run the
// server using the config path passed via `--config`.  Without flags it
// prints help and exits zero so CI can sanity-check the binary produces
// no panics.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"go.temporal.io/server/temporal"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/pkg/plugin"
)

// defaultServices is the canonical single-node Temporal service set.
// Running all four from one process is the "Temporalite" / "server
// development" pattern and is the common shape for smoke-testing a
// persistence backend end-to-end. Operators targeting a split
// deployment (separate frontend/history/matching/worker binaries)
// supply --services to override.
var defaultServices = []string{"history", "matching", "frontend", "worker"}

func main() {
	fs := flag.NewFlagSet("temporal-mongodb-server", flag.ContinueOnError)
	configPath := fs.String("config", "", "Path to Temporal server config directory (contains development.yaml)")
	env := fs.String("env", "development", "Config environment (matches <env>.yaml inside --config)")
	zone := fs.String("zone", "", "Config zone (appended to config file lookup)")
	services := fs.String("services", strings.Join(defaultServices, ","),
		"Comma-separated service names to start (history,matching,frontend,worker)")
	start := fs.Bool("start", false, "Start the server (default: print wiring info and exit)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}

	if !*start {
		fmt.Fprintln(os.Stdout, "temporal-mongodb-server: Temporal server with MongoDB persistence plugin")
		fmt.Fprintln(os.Stdout, "  --config <dir>      Temporal config directory (contains <env>.yaml)")
		fmt.Fprintln(os.Stdout, "  --env <name>        Config environment (default: development)")
		fmt.Fprintln(os.Stdout, "  --zone <name>       Config zone (optional)")
		fmt.Fprintln(os.Stdout, "  --services <list>   Services to start (default: all four)")
		fmt.Fprintln(os.Stdout, "  --start             Start the server (without this, prints help and exits 0)")
		return
	}

	// Build the interrupt channel first so signals reach NewServer +
	// Start + Stop through a single owner.
	sigCh := make(chan any, 1)
	go func() {
		osSig := make(chan os.Signal, 1)
		signal.Notify(osSig, os.Interrupt, syscall.SIGTERM)
		s := <-osSig
		fmt.Fprintf(os.Stderr, "temporal-mongodb-server: received %v; shutting down\n", s)
		sigCh <- s
	}()

	opts := []temporal.ServerOption{
		temporal.WithCustomDataStoreFactory(plugin.NewFactory()),
		temporal.WithCustomVisibilityStoreFactory(plugin.NewVisibilityFactory()),
		temporal.ForServices(splitCSV(*services)),
		temporal.InterruptOn(sigCh),
	}
	if *configPath != "" {
		opts = append(opts, temporal.WithConfigLoader(*configPath, *env, *zone))
	}

	server, err := temporal.NewServer(opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "temporal-mongodb-server: build failed: %v\n", err)
		os.Exit(1)
	}
	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "temporal-mongodb-server: start failed: %v\n", err)
		os.Exit(1)
	}
	// Server.Start blocks until InterruptOn fires; on return the server
	// has already drained. Stop is an idempotent belt-and-braces call.
	_ = server.Stop()
}

// splitCSV trims whitespace and drops empty entries. Cheaper than
// importing a full CSV parser for a tiny flag shape.
func splitCSV(in string) []string {
	out := make([]string, 0, 4)
	for _, s := range strings.Split(in, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
