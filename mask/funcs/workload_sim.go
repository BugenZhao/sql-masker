package funcs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"golang.org/x/crypto/blake2b"
)

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

func resizeInt64(i int64, tp *types.FieldType) int64 {
	switch tp.Tp {
	case mysql.TypeTiny:
		return i % math.MaxInt8
	case mysql.TypeShort:
		return i % math.MaxInt16
	case mysql.TypeInt24:
		return i % mysql.MaxInt24
	case mysql.TypeLong:
		return i % math.MaxInt32
	case mysql.TypeLonglong:
		return i % math.MaxInt64
	default:
		panic("unreachable")
	}
}

func resizeUint64(i uint64, tp *types.FieldType) uint64 {
	switch tp.Tp {
	case mysql.TypeTiny:
		return i % math.MaxUint8
	case mysql.TypeShort:
		return i % math.MaxUint16
	case mysql.TypeInt24:
		return i % mysql.MaxUint24
	case mysql.TypeLong:
		return i % math.MaxUint32
	case mysql.TypeLonglong:
		return i % math.MaxUint64
	default:
		panic("unreachable")
	}
}

func hashBytes(data interface{}, size int) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, data)
	digest, err := blake2b.New(8, nil)
	if err != nil {
		panic(err)
	}
	digest.Write(buf.Bytes())

	sum := []byte{}
	sum = digest.Sum(sum)
	return sum
}

func hashUint64(data interface{}) uint64 {
	sum := hashBytes(data, 8)
	u := binary.LittleEndian.Uint64(sum)
	return u
}

// todo: this is hardly useful since it often leads to unreasonable exponent
func hashFloat64(f float64) float64 {
	u := hashUint64(f)
	return math.Float64frombits(u)
}

func hashDecimal(d *types.MyDecimal) (*types.MyDecimal, error) {
	prec, frac := d.PrecisionAndFrac()
	f, err := d.ToFloat64()
	if err != nil {
		return nil, err
	}
	f = hashFloat64(f)

	neg := f < 0
	f = math.Abs(f)
	s := strconv.FormatFloat(f, 'f', frac, 64)
	tokens := strings.Split(s, ".")

	left := tokens[0]
	if len(left) > prec-frac {
		left = left[len(left)-(prec-frac):]
	}
	right := tokens[1]
	if len(right) > frac {
		right = right[:frac]
	}

	prefix := ""
	if neg {
		prefix = "-"
	}
	s = fmt.Sprintf("%s%s.%s", prefix, left, right)

	err = d.FromString([]byte(s))
	if err != nil {
		return nil, err
	}
	return d, nil
}

func WorkloadSimMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	switch datum.Kind() {
	case types.KindInt64:
		u := hashUint64(datum.GetInt64())
		datum.SetInt64(resizeInt64(int64(u), tp))
		return datum, tp, nil

	case types.KindUint64:
		u := hashUint64(datum.GetUint64())
		datum.SetUint64(resizeUint64(u, tp))
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

	default:
		return types.NewDatum("unimplemented"), types.NewFieldType(mysql.TypeString), nil
	}
}
