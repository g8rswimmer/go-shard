//go:build integration

// Package examples runs every example against real PostgreSQL shards, so they
// cannot drift from the API: each must exit 0 and print its key lines.
package examples

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/g8rswimmer/go-shard/shardtest"
)

// What each example must print. These are the lines its README explains; the
// full output is in the README.
var examples = []struct {
	name string
	want []string
}{
	{"quickstart", []string{
		"wrote profile 6 (Margaret) to shard-01",
		"read profile 3: Edsger",
		"refused: shard: no shard key",
	}},
	{"colocation", []string{
		"Ada lives in London",
		"profiles named Grace afterwards: 0",
		"Barbara was saved: 1, the stray was not: 0",
	}},
	{"fanout", []string{
		"player-27\t99",
		"blue\t10\t505",
		"with AllowPartial: 24 players from the shards that answered",
	}},
	{"writes", []string{
		"shard-03: wrote 6 rows",
		"total rows reported: 12 (all 12, none written twice)",
	}},
	{"explain", []string{
		"strategy:  single",
		"1. OrderedMerge(name)",
		"5. DropHidden(2)",
	}},
	{"migrations", []string{
		"shard-02: 2 (behind)",
		"drift: yes",
		"shard-03: 3 countries",
	}},
	{"observability", []string{
		"strategy=all targets=shard-01,shard-02,shard-03",
		"event merge",
		"shard-01: healthy=true",
	}},
}

func TestExamplesRun(t *testing.T) {
	cluster := shardtest.NewCluster(t, 3)
	var dsns []string
	for _, id := range cluster.IDs() {
		dsns = append(dsns, cluster.DSN(id))
	}

	for _, ex := range examples {
		t.Run(ex.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, "go", "run", "./examples/"+ex.name)
			cmd.Dir = ".."
			cmd.Env = append(cmd.Environ(), "SHARD_DSNS="+strings.Join(dsns, ","))
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("exit: %v\n%s", err, out)
			}
			for _, want := range ex.want {
				if !strings.Contains(string(out), want) {
					t.Errorf("output does not contain %q:\n%s", want, out)
				}
			}
		})
	}
}
