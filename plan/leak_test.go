//go:build !integration

package plan

import (
	"testing"

	"go.uber.org/goleak"
)

// No test may leave a goroutine behind: they would be leaked connections,
// merges or hooks in a program using the library.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
