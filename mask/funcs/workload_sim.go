package funcs

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	gotime "time"
	"unicode"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
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
	maskItemPrefix = "I"
)

var maskStmtCtx = mock.NewContext().GetSessionVars().StmtCtx

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
		_ = binary.Write(buf, binary.LittleEndian, data)
		bs = buf.Bytes()
	}

	hasher := newHasher()
	_, _ = hasher.Write(bs)

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

func hashFloat64(f float64) (float64, error) {
	neg := f < 0
	f = math.Abs(f)
	// If the fraction precision is less than the default precision 6, there will
	// be some extra `0` padding like 12.300000, so `TrimRight` here
	tokens := strings.Split(strings.TrimRight(fmt.Sprintf("%f", f), "0"), ".")
	maskedFloat := hashFloat64Raw(f)

	frac := 0
	if len(tokens) >= 2 {
		frac = len(tokens[1])
	}
	res := formatFloat(maskedFloat, neg, len(tokens[0]), frac)
	f, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return f, err
	}
	return f, nil
}

func hashFloat64Raw(f float64) float64 {
	u := hashUint64(f)
	f = math.Float64frombits(u)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		f = float64(u)
	}
	return f
}

func formatFloat(f float64, neg bool, intNum, frac int) string {
	f = math.Abs(f)
	if intNum < 1 {
		intNum = 1
	}

	sb := strings.Builder{}
	for _, c := range fmt.Sprintf("%.10e", f) {
		if unicode.IsNumber(c) {
			_, _ = sb.WriteRune(c)
		}
	}
	str := sb.String()

	// fill "0" to trail
	if len(str) < intNum {
		str += strings.Repeat("0", intNum-len(str))
	}
	left := str[:intNum]

	rightEnd := intNum + frac
	if rightEnd > len(str) {
		rightEnd = len(str)
	}
	right := str[intNum:rightEnd]

	var res string
	if len(right) != 0 {
		res = fmt.Sprintf("%s.%s", left, right)
	} else {
		res = left
	}

	if neg {
		res = fmt.Sprintf("-%s", res)
	}
	return res
}

func hashDecimal(d *types.MyDecimal) (*types.MyDecimal, error) {
	neg := d.IsNegative()
	prec, frac := d.PrecisionAndFrac()
	f, err := d.ToFloat64()
	if err != nil {
		return nil, err
	}
	f = math.Abs(f)

	res := formatFloat(hashFloat64Raw(f), neg, prec-frac, frac)
	err = d.FromString([]byte(res))
	if err != nil {
		return nil, fmt.Errorf("failed to parse decimal `%s`; %w", res, err)
	}

	return d, nil
}

func maskString(s []byte) string {
	size := len(s)
	if size < 2 {
		size = 2
	}

	sum := hashBytes([]byte(s), size/2)
	hex := hex.EncodeToString(sum)
	hex = hex + strings.Repeat("*", size-len(hex))
	return hex
}

func maskDuration(d types.Duration) (types.Duration, error) {
	// hack: 3e15 is slightly smaller than the max duration (838:59:59) * 10^9 nanosecs
	maskedDuration := maskInt64(int64(d.Duration)) % 3e15

	return types.Duration{
		Duration: gotime.Duration(maskedDuration),
		Fsp:      types.MaxFsp,
	}.RoundFrac(d.Fsp, gotime.UTC) // call RoundFrac to round the `gotime.Duration`
}

func maskTime(t types.Time) (types.Time, error) {
	fsp := t.Fsp()
	tp := t.Type()

	uncheckedTime := maskUint64(uint64(t.CoreTime()))
	t.SetCoreTime(types.CoreTime(uncheckedTime))

	year := 0
	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime:
		year = t.Year() % 10000 // 0..9999
	case mysql.TypeTimestamp:
		// Hack! the timestamp is the number of non-leap seconds since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp").
		// the valid range is 0..(1 << 31) - 1, 2035 is an approximately upper bound
		year = t.Year()%(2036-1970) + 1970 // 1970..2035
	}
	month := (t.Month() % 12) + 1                      // 1..12
	day := (t.Day() % lastDayOfMonth(year, month)) + 1 // 1..28/29/30/31
	hour := t.Hour() % 24                              // 0..23
	minute := t.Minute() % 60                          // 0..59
	second := t.Second() % 60                          // 0..59
	micro := t.Microsecond() % 1000000                 // 0..999999

	maskedTime := types.NewTime(types.FromDate(year, month, day, hour, minute, second, micro), tp, fsp)

	// TODO: get a `StatementContext` and do checking here
	// err := maskedTime.Check(ctx)

	return maskedTime, nil
}

func lastDayOfMonth(year, month int) int {
	day := 0
	switch month {
	case 4, 6, 9, 11:
		day = 30
	case 2:
		day = 28
		//  leap year
		if (year%4 == 0 && year%100 != 0) || year%400 == 0 {
			day += 1
		}
	default:
		day = 31
	}
	return day
}

func maskItem(l, id int) string {
	res := fmt.Sprintf("%s%v", maskItemPrefix, id)
	if l > len(res) {
		res = res + strings.Repeat("*", l-len(res))
	}
	return res
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
		f, err := hashFloat64(datum.GetFloat64())
		if err != nil {
			return datum, tp, err
		}
		datum.SetFloat64(f)
		return datum, tp, nil

	case types.KindFloat32:
		f64, err := hashFloat64(float64(datum.GetFloat32()))
		if err != nil {
			return datum, tp, err
		}
		datum.SetFloat32(float32(f64))
		return datum, tp, nil

	case types.KindMysqlDecimal:
		d, err := hashDecimal(datum.GetMysqlDecimal())
		if err != nil {
			return datum, tp, err
		}
		datum.SetMysqlDecimal(d)
		return datum, tp, nil

	case types.KindString:
		s := maskString([]byte(datum.GetString()))
		datum.SetString(s, datum.Collation())
		return datum, tp, nil

	case types.KindBytes:
		s := maskString(datum.GetBytes())
		datum.SetBytes([]byte(s))
		return datum, tp, nil

	case types.KindMysqlEnum:
		e := datum.GetMysqlEnum()
		e.Name = maskItem(len(e.Name), int(e.Value))
		datum.SetMysqlEnum(e, datum.Collation())
		return datum, tp, nil

	case types.KindMysqlSet:
		s := datum.GetMysqlSet()
		var items []string
		for i, e := range strings.Split(s.Name, ",") {
			items = append(items, maskItem(len(e), i))
		}
		s.Name = strings.Join(items, ",")
		datum.SetMysqlSet(s, datum.Collation())
		return datum, tp, nil

	case types.KindMysqlDuration:
		d, err := maskDuration(datum.GetMysqlDuration())
		if err != nil {
			return datum, tp, err
		}
		datum.SetMysqlDuration(d)
		return datum, tp, nil

	case types.KindMysqlTime:
		t, err := maskTime(datum.GetMysqlTime())
		if err != nil {
			return datum, tp, err
		}
		datum.SetMysqlTime(t)
		return datum, tp, nil

	default:
		// unimplemented for this type, ignore for now
		return datum, tp, nil
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
