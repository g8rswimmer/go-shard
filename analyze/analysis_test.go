package analyze

import "testing"

func TestOpKind(t *testing.T) {
	tests := []struct {
		op    OpKind
		name  string
		write bool
	}{
		{OpSelect, "SELECT", false},
		{OpInsert, "INSERT", true},
		{OpUpdate, "UPDATE", true},
		{OpDelete, "DELETE", true},
		{OpOther, "other", false},
		{OpKind(0), "OpKind(?)", false},
	}
	for _, tc := range tests {
		if tc.op.String() != tc.name || tc.op.IsWrite() != tc.write {
			t.Errorf("%d: String %q IsWrite %v; want %q %v", tc.op, tc.op.String(), tc.op.IsWrite(), tc.name, tc.write)
		}
	}
}
