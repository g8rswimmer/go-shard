package merge

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// compare orders two non-NULL values. numeric says that strings are decimal
// numbers (PostgreSQL's numeric type arrives as text).
func compare(a, b any, numeric bool) (int, error) {
	switch x := a.(type) {
	case string:
		if y, ok := b.(string); ok && !numeric {
			return strings.Compare(x, y), nil
		}
	case []byte:
		if y, ok := b.([]byte); ok {
			return bytes.Compare(x, y), nil
		}
	case bool:
		if y, ok := b.(bool); ok {
			switch {
			case x == y:
				return 0, nil
			case !x:
				return -1, nil
			default:
				return 1, nil
			}
		}
	case time.Time:
		if y, ok := b.(time.Time); ok {
			return x.Compare(y), nil
		}
	default:
		// numbers are handled below
	}

	if ra, rb := toRat(a), toRat(b); ra != nil && rb != nil {
		return ra.Cmp(rb), nil
	}
	return 0, fmt.Errorf("shard: cannot order %T against %T while merging", a, b)
}

// toRat converts a number to an exact rational, or returns nil.
func toRat(v any) *big.Rat {
	switch x := v.(type) {
	case int64:
		return new(big.Rat).SetInt64(x)
	case int:
		return new(big.Rat).SetInt64(int64(x))
	case int32:
		return new(big.Rat).SetInt64(int64(x))
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil
		}
		return new(big.Rat).SetFloat64(x)
	case string:
		r, ok := new(big.Rat).SetString(x)
		if !ok {
			return nil
		}
		return r
	case []byte:
		return toRat(string(x))
	default:
		return nil
	}
}

// add sums two non-NULL values of the same kind.
func add(a, b any) (any, error) {
	switch x := a.(type) {
	case int64:
		if y, ok := b.(int64); ok {
			return x + y, nil
		}
	case float64:
		if y, ok := b.(float64); ok {
			return x + y, nil
		}
	case string:
		if y, ok := b.(string); ok {
			ra, rb := toRat(x), toRat(y)
			if ra != nil && rb != nil {
				return new(big.Rat).Add(ra, rb).FloatString(max(scaleOf(x), scaleOf(y))), nil
			}
		}
	default:
		// not summable
	}
	return nil, fmt.Errorf("shard: cannot add %T and %T while merging", a, b)
}

// scaleOf is the number of digits after the decimal point in a decimal string.
func scaleOf(s string) int {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return len(s) - i - 1
	}
	return 0
}

// avgScale is the number of decimal places PostgreSQL gives the average of
// integers.
const avgScale = 16

// average divides a sum by a count.
func average(sum, count any) (any, error) {
	n, ok := count.(int64)
	if sum == nil || !ok || n == 0 {
		return nil, nil
	}
	switch s := sum.(type) {
	case float64:
		return s / float64(n), nil
	default:
		r := toRat(sum)
		if r == nil {
			return nil, fmt.Errorf("shard: cannot average %T while merging", sum)
		}
		scale := avgScale
		if str, ok := sum.(string); ok {
			scale = max(avgScale, scaleOf(str))
		}
		return new(big.Rat).Quo(r, new(big.Rat).SetInt64(n)).FloatString(scale), nil
	}
}

// assign stores a column value in a Scan destination, converting as
// database/sql does for the common cases.
func assign(dest, src any) error {
	if sc, ok := dest.(sql.Scanner); ok {
		return sc.Scan(src)
	}
	if d, ok := dest.(*any); ok {
		if b, isBytes := src.([]byte); isBytes {
			src = bytes.Clone(b)
		}
		*d = src
		return nil
	}

	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("shard: Scan needs a non-nil pointer, got %T", dest)
	}
	elem := rv.Elem()
	if elem.Kind() == reflect.Pointer {
		// *T, as database/sql allows: NULL leaves it nil, anything else gets a T.
		if src == nil {
			elem.SetZero()
			return nil
		}
		target := reflect.New(elem.Type().Elem())
		if err := assign(target.Interface(), src); err != nil {
			return err
		}
		elem.Set(target)
		return nil
	}
	if src == nil {
		return fmt.Errorf("shard: cannot scan NULL into %T; scan into a pointer or a sql.Null type", dest)
	}
	if sv := reflect.ValueOf(src); sv.Type().AssignableTo(elem.Type()) {
		if b, isBytes := src.([]byte); isBytes {
			src = bytes.Clone(b)
			sv = reflect.ValueOf(src)
		}
		elem.Set(sv)
		return nil
	}

	switch elem.Kind() {
	case reflect.String:
		elem.SetString(asString(src))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := strconv.ParseInt(asString(src), 10, 64)
		if err != nil || elem.OverflowInt(i) {
			return fmt.Errorf("shard: cannot scan %v into %T", src, dest)
		}
		elem.SetInt(i)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u, err := strconv.ParseUint(asString(src), 10, 64)
		if err != nil || elem.OverflowUint(u) {
			return fmt.Errorf("shard: cannot scan %v into %T", src, dest)
		}
		elem.SetUint(u)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(asString(src), 64)
		if err != nil || elem.OverflowFloat(f) {
			return fmt.Errorf("shard: cannot scan %v into %T", src, dest)
		}
		elem.SetFloat(f)
	case reflect.Bool:
		b, err := strconv.ParseBool(asString(src))
		if err != nil {
			return fmt.Errorf("shard: cannot scan %v into %T", src, dest)
		}
		elem.SetBool(b)
	default:
		return fmt.Errorf("shard: cannot scan %T into %T", src, dest)
	}
	return nil
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}
