package router

import (
	"errors"
	"math"
	"testing"
)

type namedUUID [16]byte // stands in for uuid.UUID, whose underlying type is [16]byte

type namedInt int32

// TestBucketVectors pins the bucket of known keys. The expected values were
// computed with an independent xxhash64 implementation (not this package), so
// they also prove the canonical encoding. If this test fails, routing has
// changed and existing data would be looked up on the wrong shard: do not
// update the numbers, fix the code.
func TestBucketVectors(t *testing.T) {
	r, err := New(Even("a")...)
	if err != nil {
		t.Fatal(err)
	}
	uuidBytes := namedUUID{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}

	tests := []struct {
		name string
		key  any
		want int
	}{
		{"int 0", 0, 630},
		{"int 1", 1, 1008},
		{"int 42", 42, 678},
		{"int -1", -1, 182},
		{"int max", int64(math.MaxInt64), 202},
		{"empty string", "", 950},
		{"string", "profile-42", 368},
		{"unicode string", "héllo wörld", 682},
		{"uuid text", "123e4567-e89b-12d3-a456-426614174000", 650},
		{"uuid text upper", "123E4567-E89B-12D3-A456-426614174000", 650},
		{"uuid bytes", uuidBytes, 650},
		{"uuid zero", "00000000-0000-0000-0000-000000000000", 934},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.Bucket(tc.key)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("Bucket(%v) = %d, want %d", tc.key, got, tc.want)
			}
		})
	}
}

func TestIntegerTypesAgree(t *testing.T) {
	r, _ := New(Even("a")...)
	want, _ := r.Bucket(42)
	n := 42
	for _, k := range []any{int8(42), int16(42), int32(42), int64(42), uint(42), uint8(42), uint16(42), uint32(42), uint64(42), namedInt(42), &n} {
		got, err := r.Bucket(k)
		if err != nil || got != want {
			t.Errorf("Bucket(%T(42)) = %d, %v; want %d", k, got, err, want)
		}
	}
}

func TestIntAndStringDiffer(t *testing.T) {
	a, _ := Canonical(5)
	b, _ := Canonical("5")
	if string(a) == string(b) {
		t.Error("int 5 and string \"5\" must not share a canonical encoding")
	}
}

func TestNonCanonicalUUIDStringsAreOrdinaryStrings(t *testing.T) {
	for _, s := range []string{
		"123e4567e89b12d3a456426614174000",       // no dashes
		"{123e4567-e89b-12d3-a456-426614174000}", // braces
		"123e4567-e89b-12d3-a456-42661417400g",   // not hex
		"123e4567-e89b-12d3-a456-4266141740000",  // too long
		"123e4567-e89b-12d3-a456-42661417400",    // too short
		"123e4567xe89b-12d3-a456-426614174000",   // wrong separator
	} {
		c, err := Canonical(s)
		if err != nil {
			t.Fatal(err)
		}
		if c[0] != tagString {
			t.Errorf("Canonical(%q) tag = %#x, want string tag", s, c[0])
		}
	}
}

func TestCanonicalErrors(t *testing.T) {
	var nilPtr *int
	tests := []struct {
		name string
		key  any
		want error
	}{
		{"nil", nil, ErrNilKey},
		{"nil pointer", nilPtr, ErrNilKey},
		{"uint64 too big", uint64(math.MaxInt64) + 1, ErrKeyOutOfRange},
		{"float", 1.5, ErrUnsupportedKey},
		{"bool", true, ErrUnsupportedKey},
		{"bytes", []byte("abc"), ErrUnsupportedKey},
		{"struct", struct{}{}, ErrUnsupportedKey},
		{"short array", [4]byte{}, ErrUnsupportedKey},
		{"map", map[string]int{}, ErrUnsupportedKey},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Canonical(tc.key); !errors.Is(err, tc.want) {
				t.Errorf("Canonical(%v) error = %v, want %v", tc.key, err, tc.want)
			}
		})
	}
}
