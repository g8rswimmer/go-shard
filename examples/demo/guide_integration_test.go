//go:build integration

package main

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// block is one fenced code block of the guide.
type block struct {
	info string // text after the opening fence: "sh", "sh skip", "text"
	text string
	line int
}

// TestDemoGuide runs the commands in docs/DEMO.md, in order, from the
// repository root and compares each one's output with the `text` block that
// follows it, so the guide cannot go stale. It needs the shards from `make up`
// in the state `make demo-reset` leaves them, so it only runs when asked
// (`make demo-check`).
func TestDemoGuide(t *testing.T) {
	if os.Getenv("DEMO_GUIDE") == "" {
		t.Skip("set DEMO_GUIDE=1 (make demo-check) to run the guide against the make up shards")
	}
	blocks := readBlocks(t, "../../docs/DEMO.md")

	ran := 0
	for i, b := range blocks {
		if b.info != "sh" {
			continue
		}
		cmdline := strings.TrimSpace(b.text)
		t.Run(cmdline, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, "sh", "-c", cmdline)
			cmd.Dir = "../.."
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("DEMO.md line %d: %v\n%s", b.line, err, out)
			}
			ran++

			if i+1 >= len(blocks) || blocks[i+1].info != "text" {
				return // only has to succeed
			}
			want := normalize(blocks[i+1].text)
			if got := normalize(string(out)); got != want {
				t.Errorf("DEMO.md line %d: output differs from the guide\n--- guide\n%s\n--- got\n%s", blocks[i+1].line, want, got)
			}
		})
	}
	if ran == 0 {
		t.Fatal("no commands found in DEMO.md")
	}
}

// normalize ignores trailing spaces and blank lines at the ends.
func normalize(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(l, " \t\r")
	}
	return strings.Join(lines, "\n")
}

func readBlocks(t *testing.T, path string) []block {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var blocks []block
	var cur *block
	var body []string
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		switch {
		case cur == nil && strings.HasPrefix(line, "```"):
			cur = &block{info: strings.TrimSpace(strings.TrimPrefix(line, "```")), line: n}
			body = nil
		case cur != nil && line == "```":
			cur.text = strings.Join(body, "\n")
			blocks = append(blocks, *cur)
			cur = nil
		case cur != nil:
			body = append(body, line)
		default:
			// prose
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return blocks
}
