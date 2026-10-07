package router

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
)

// Canonical key encoding. The bytes produced here are hashed to pick a shard,
// so they must never change: changing them moves data to different shards.
// Fixed test vectors in key_test.go guard this.
//
//	integer  0x01 + 8-byte big-endian int64
//	uuid     0x02 + 16 bytes
//	string   0x03 + UTF-8 bytes
const (
	tagInt    byte = 0x01
	tagUUID   byte = 0x02
	tagString byte = 0x03
)

var (
	// ErrNilKey is returned for a nil shard key.
	ErrNilKey = errors.New("router: shard key is nil")
	// ErrUnsupportedKey is returned for a key whose type cannot be routed.
	ErrUnsupportedKey = errors.New("router: unsupported shard key type")
	// ErrKeyOutOfRange is returned for an unsigned key above math.MaxInt64.
	ErrKeyOutOfRange = errors.New("router: shard key is out of range")
)

// Canonical returns the stable byte encoding of a shard key.
//
// Supported keys:
//   - any integer type, hashed as int64 (so int32(5) and int64(5) agree);
//     unsigned values above math.MaxInt64 are rejected
//   - strings, hashed as UTF-8
//   - UUIDs, as a [16]byte (the underlying type of github.com/google/uuid.UUID)
//     or as a string in canonical 8-4-4-4-12 form (any letter case). Both forms
//     of the same UUID reach the same shard, so it does not matter whether a
//     caller holds the value as bytes or text.
//   - pointers to any of the above (nil is ErrNilKey)
func Canonical(key any) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}
	v := reflect.ValueOf(key)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, ErrNilKey
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return encodeInt(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := v.Uint()
		if u > math.MaxInt64 {
			return nil, fmt.Errorf("%w: %d does not fit in int64", ErrKeyOutOfRange, u)
		}
		return encodeInt(int64(u)), nil
	case reflect.String:
		s := v.String()
		if u, ok := parseUUID(s); ok {
			return append([]byte{tagUUID}, u[:]...), nil
		}
		return append([]byte{tagString}, s...), nil
	case reflect.Array:
		if v.Len() == 16 && v.Type().Elem().Kind() == reflect.Uint8 {
			out := make([]byte, 17)
			out[0] = tagUUID
			for i := 0; i < 16; i++ {
				out[i+1] = byte(v.Index(i).Uint())
			}
			return out, nil
		}
	default:
		// not a routable kind
	}
	return nil, fmt.Errorf("%w: %T", ErrUnsupportedKey, key)
}

func encodeInt(n int64) []byte {
	out := make([]byte, 9)
	out[0] = tagInt
	binary.BigEndian.PutUint64(out[1:], uint64(n))
	return out
}

// parseUUID parses the canonical 8-4-4-4-12 form. Other layouts (braces, urn:
// prefix, no dashes) are treated as ordinary strings.
func parseUUID(s string) ([16]byte, bool) {
	var u [16]byte
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, false
	}
	n := 0
	for i := 0; i < len(s); {
		if s[i] == '-' {
			i++
			continue
		}
		hi, ok1 := unhex(s[i])
		lo, ok2 := unhex(s[i+1])
		if !ok1 || !ok2 {
			return u, false
		}
		u[n] = hi<<4 | lo
		n++
		i += 2
	}
	return u, true
}

func unhex(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}
