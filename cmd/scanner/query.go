package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/config"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage/scylla"
)

const queryUsage = `Usage:
  scanner query did <did> [--limit N]
        Print recent events for a DID, newest first.
  scanner query flagged [--since 10m] [--limit N]
        Print recent detector hits across all rules.
`

// runQuery is the entry point for the read-side CLI. It opens a non-batched
// Scylla session (writes aren't needed for queries) and dispatches to the
// matching subcommand. Returning an error rather than calling os.Exit keeps
// the caller (main) in charge of process exit codes.
func runQuery(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, queryUsage)
		return fmt.Errorf("missing query subcommand")
	}

	cfg := config.MustLoad(os.Getenv("ENV"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := scylla.New(ctx, scylla.Config{
		Hosts:       cfg.ScyllaHosts,
		Keyspace:    cfg.ScyllaKeyspace,
		Consistency: cfg.ScyllaConsistency,
		Timeout:     time.Duration(cfg.ScyllaTimeoutMS) * time.Millisecond,
		NumConns:    cfg.ScyllaNumConns,
	})
	if err != nil {
		return fmt.Errorf("connect scylla: %w", err)
	}
	defer store.Close()

	switch args[0] {
	case "did":
		return queryDID(ctx, store, os.Stdout, args[1:])
	case "flagged":
		return queryFlagged(ctx, store, os.Stdout, args[1:])
	default:
		fmt.Fprint(os.Stderr, queryUsage)
		return fmt.Errorf("unknown query subcommand: %s", args[0])
	}
}

func queryDID(ctx context.Context, store *scylla.Store, out io.Writer, args []string) error {
	fs := flag.NewFlagSet("query did", flag.ContinueOnError)
	limit := fs.Int("limit", 25, "maximum rows to return")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("query did: expected <did>")
	}
	did := fs.Arg(0)

	rows, err := store.RecentEventsByDID(ctx, did, *limit)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Fprintf(out, "no events for %s\n", did)
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "RECEIVED_AT\tID\tKIND\tTEXT")
	for _, r := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			r.ReceivedAt.UTC().Format(time.RFC3339),
			r.ID,
			r.Kind,
			truncate(r.Text, 80),
		)
	}
	return tw.Flush()
}

func queryFlagged(ctx context.Context, store *scylla.Store, out io.Writer, args []string) error {
	fs := flag.NewFlagSet("query flagged", flag.ContinueOnError)
	since := fs.Duration("since", 10*time.Minute, "look back window (e.g. 5m, 1h)")
	limit := fs.Int("limit", 50, "maximum rows to return")
	if err := fs.Parse(args); err != nil {
		return err
	}

	rows, err := store.RecentFlagged(ctx, *since, *limit)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Fprintf(out, "no flagged events in the last %s\n", *since)
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "RECEIVED_AT\tRULE\tSEVERITY\tDID\tREASON")
	for _, r := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			r.ReceivedAt.UTC().Format(time.RFC3339),
			r.RuleID,
			r.Severity,
			r.DID,
			truncate(r.Reason, 60),
		)
	}
	return tw.Flush()
}

func truncate(s string, max int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= max {
		return s
	}
	return s[:max-1] + "…"
}
