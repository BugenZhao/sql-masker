package funcs

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"

	lj "github.com/LianjiaTech/d18n/mask"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/zeebo/blake3"
)

/*
TypeTiny        byte = 1
TypeShort       byte = 2
TypeLong        byte = 3
TypeFloat       byte = 4
TypeDouble      byte = 5
TypeNull        byte = 6
TypeTimestamp   byte = 7
TypeLonglong    byte = 8
TypeInt24       byte = 9
TypeDate        byte = 10
*/

const (
	defaultContext = "tidb"
)

func newHasher() *blake3.Hasher {
	hasher := blake3.NewDeriveKey(defaultContext)
	return hasher
}

func hashBytes(data interface{}, size int) []byte {
	var bs []byte
	switch data := data.(type) {
	case []byte:
		bs = data
	default:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, data)
		bs = buf.Bytes()
	}

	hasher := newHasher()
	hasher.Write(bs)

	sum := make([]byte, size)
	n, err := hasher.Digest().Read(sum)
	if err != nil {
		panic(err)
	}
	if n != size {
		panic(fmt.Sprintf("bad size `%d` vs `%d`", n, size))
	}

	return sum
}

func hashUint64(data interface{}) uint64 {
	sum := hashBytes(data, 8)
	u := binary.LittleEndian.Uint64(sum)
	return u
}

func maskUint64(from uint64) uint64 {
	to := binary.LittleEndian.Uint64(hashBytes(from, 8))

	if from <= math.MaxUint8 {
		return to % (math.MaxUint8 + 1)
	} else if from <= math.MaxUint16 {
		return to % (math.MaxUint16 + 1)
	} else if from <= mysql.MaxUint24 {
		return to % (mysql.MaxUint24 + 1)
	} else if from <= math.MaxUint32 {
		return to % (math.MaxUint32 + 1)
	} else {
		return to
	}
}

func maskInt64(from int64) int64 {
	to := int64(binary.LittleEndian.Uint64(hashBytes(from, 8)))

	if from <= math.MaxInt8 && from >= math.MinInt8 {
		return to % (math.MaxInt8 + 1)
	} else if from <= math.MaxInt16 && from >= math.MinInt16 {
		return to % (math.MaxInt16 + 1)
	} else if from <= mysql.MaxInt24 && from >= mysql.MinInt24 {
		return to % (mysql.MaxInt24 + 1)
	} else if from <= math.MaxInt32 && from >= math.MinInt32 {
		return to % (math.MaxInt32 + 1)
	} else {
		return to
	}
}

// todo: this is almost nonsense since it often leads to unreasonable exponent
func hashFloat64(f float64) float64 {
	u := hashUint64(f)
	return math.Float64frombits(u)
}

// todo: this is NOT PURE
// func maskFloat64(f float64) float64 {
// 	s, _ := lj.LaplaceDPFloat64(f, 100, 1, 1, 0)
// 	f, _ = strconv.ParseFloat(s, 64)
// 	return f
// }

// todo: same as hashFloat64, really a bad job
func hashDecimal(d *types.MyDecimal) (*types.MyDecimal, error) {
	prec, frac := d.PrecisionAndFrac()
	f, err := d.ToFloat64()
	if err != nil {
		return nil, err
	}
	f = hashFloat64(f)

	if math.IsNaN(f) || math.IsInf(f, 0) {
		f = 0
	}

	neg := f < 0
	f = math.Abs(f)
	s := strconv.FormatFloat(f, 'f', frac, 64)
	tokens := strings.Split(s, ".")

	left := tokens[0]
	if len(left) > prec-frac {
		left = left[len(left)-(prec-frac):]
	}
	right := "0"
	if len(tokens) >= 2 {
		right = tokens[1]
	}
	if len(right) > frac {
		right = right[:frac]
	}

	prefix := ""
	if neg {
		prefix = "-"
	}
	if right == "" {
		s = fmt.Sprintf("%s%s", prefix, left)
	} else {
		s = fmt.Sprintf("%s%s.%s", prefix, left, right)
	}

	err = d.FromString([]byte(s))
	if err != nil {
		return nil, fmt.Errorf("failed to parse decimal `%s`; %w", s, err)
	}
	return d, nil
}

func maskString(s []byte) string {
	size := len(s)

	sum := hashBytes([]byte(s), size/2)
	hex := hex.EncodeToString(sum)
	hex = hex + strings.Repeat("*", size-len(hex))
	return hex
}

func maskTime(t types.Time) string {
	rounded, _ := lj.DateRound(t.String(), "day")
	return rounded
}

func WorkloadSimMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	switch datum.Kind() {
	case types.KindInt64:
		datum.SetInt64(maskInt64(datum.GetInt64()))
		return datum, tp, nil

	case types.KindUint64:
		datum.SetUint64(maskUint64(datum.GetUint64()))
		return datum, tp, nil

	case types.KindFloat64:
		f := hashFloat64(datum.GetFloat64())
		datum.SetFloat64(f)
		return datum, tp, nil

	case types.KindFloat32:
		f := float32(hashFloat64(float64(datum.GetFloat32())))
		datum.SetFloat32(f)
		return datum, tp, nil

	case types.KindMysqlDecimal:
		d := datum.GetMysqlDecimal()
		_, err := hashDecimal(d)
		return datum, tp, err

	case types.KindString:
		s := maskString([]byte(datum.GetString()))
		datum.SetString(s, datum.Collation())
		return datum, tp, nil

	case types.KindBytes:
		s := maskString(datum.GetBytes())
		datum.SetBytes([]byte(s))
		return datum, tp, nil

	// it's ok to return a string, since all non-numeric types will be converted to string when serializing text events
	case types.KindMysqlTime:
		ds := maskTime(datum.GetMysqlTime())
		return types.NewDatum(ds), stringTp, nil

	default:
		return datum, tp, fmt.Errorf("unimplemented for type `%v`", tp)
	}
}

/*
KindNull          byte = 0
KindInt64         byte = 1
KindUint64        byte = 2
KindFloat32       byte = 3
KindFloat64       byte = 4
KindString        byte = 5
KindBytes         byte = 6
KindBinaryLiteral byte = 7 // Used for BIT / HEX literals.
KindMysqlDecimal  byte = 8
KindMysqlDuration byte = 9
KindMysqlEnum     byte = 10
KindMysqlBit      byte = 11 // Used for BIT table column values.
KindMysqlSet      byte = 12
KindMysqlTime     byte = 13
KindInterface     byte = 14
KindMinNotNull    byte = 15
KindMaxValue      byte = 16
KindRaw           byte = 17
KindMysqlJSON     byte = 18
*/
