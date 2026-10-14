// Command qualify-report renders an aws-qualify artifacts directory
// into report.md and prints the verdict. It exits 0 on GO and 1 on
// NO-GO.
//
//	go run ./cmd/qualify-report -dir ../../deploy/aws-qualify/artifacts/functional-<run>
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/e2e/report"
)

func main() {
	dir := flag.String("dir", "", "artifacts directory of one run")
	flag.Parse()
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "qualify-report: -dir is required")
		os.Exit(2)
	}
	rep, err := report.Render(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "qualify-report: %v\n", err)
	}
	out := filepath.Join(*dir, "report.md")
	if werr := os.WriteFile(out, []byte(rep.Markdown), 0o644); werr != nil {
		fmt.Fprintf(os.Stderr, "qualify-report: %v\n", werr)
		os.Exit(2)
	}
	fmt.Printf("%s: %s\n", rep.Verdict, out)
	if rep.Verdict != "GO" || err != nil {
		os.Exit(1)
	}
}
