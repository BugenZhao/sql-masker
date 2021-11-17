package funcs

import (
	"testing"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestWorkloadSimMask(t *testing.T) {
	t.Parallel()

	mustNewDurationDatum := func(str string, fsp int8) types.Datum {
		dur, err := types.ParseDuration(maskStmtCtx, str, fsp)
		require.Nil(t, err)
		return types.NewDurationDatum(dur)
	}
	mustNewTimeDatum := func(str string, tp byte, fsp int8) types.Datum {
		time, err := types.ParseTime(maskStmtCtx, str, tp, fsp)
		require.Nil(t, err)
		return types.NewTimeDatum(time)
	}
	mustNewDecimalDatum := func(str string) types.Datum {
		var d types.MyDecimal
		err := d.FromString([]byte(str))
		require.Nil(t, err)
		return types.NewDecimalDatum(&d)
	}

	tests := []struct {
		from     types.Datum
		expected types.Datum
	}{
		{types.NewIntDatum(42), types.NewIntDatum(-113)},                             // 8
		{types.NewIntDatum(4200), types.NewIntDatum(32405)},                          // 16
		{types.NewIntDatum(420000), types.NewIntDatum(-5612912)},                     // 24
		{types.NewIntDatum(42000000), types.NewIntDatum(-2040048546)},                // 32
		{types.NewIntDatum(420000000000), types.NewIntDatum(-1884590655846725089)},   // 64
		{types.NewUintDatum(42), types.NewUintDatum(15)},                             // 8
		{types.NewUintDatum(4200), types.NewUintDatum(65173)},                        // 16
		{types.NewUintDatum(420000), types.NewUintDatum(2775696)},                    // 24
		{types.NewUintDatum(42000000), types.NewUintDatum(107435102)},                // 32
		{types.NewUintDatum(420000000000), types.NewUintDatum(16562153417862826527)}, // 64

		{types.NewFloat64Datum(42.42), types.NewFloat64Datum(78.18)},
		{types.NewFloat64Datum(-42.42), types.NewFloat64Datum(-78.18)},
		{types.NewFloat64Datum(4242), types.NewFloat64Datum(1188)},
		{types.NewFloat64Datum(0.4242), types.NewFloat64Datum(1.0003)},
		{types.NewFloat64Datum(4.2e10), types.NewFloat64Datum(3.0508343005e+10)},
		{types.NewFloat64Datum(4.2e20), types.NewFloat64Datum(1.5036856769116e+20)},
		{types.NewFloat64Datum(4.2e-10), types.NewFloat64Datum(2)},
		{types.NewFloat64Datum(4.2e-20), types.NewFloat64Datum(9)},

		{types.NewFloat32Datum(42.42), types.NewFloat32Datum(97.13432)},

		{mustNewDecimalDatum("42.42"), mustNewDecimalDatum("78.18")},
		{mustNewDecimalDatum("-42.42"), mustNewDecimalDatum("-78.18")},
		{mustNewDecimalDatum("4242"), mustNewDecimalDatum("1188")},
		{mustNewDecimalDatum("0.4242"), mustNewDecimalDatum("1.0003")},

		{types.NewStringDatum("你好"), types.NewStringDatum("ba3468")},
		{types.NewBytesDatum([]byte("\x01\x02")), types.NewBytesDatum([]byte("0a"))},
		{types.NewMysqlEnumDatum(types.Enum{Name: "male", Value: 1}), types.NewMysqlEnumDatum(types.Enum{Name: "d85e", Value: 1})},

		{mustNewDurationDatum("10:11:12.1314", 4), mustNewDurationDatum("307:05:33.1856", 4)},
		{mustNewTimeDatum("2021-10-19", mysql.TypeDate, 0), mustNewTimeDatum("1706-05-07", mysql.TypeDate, 0)},
		{mustNewTimeDatum("2021-10-19 12:34:56.7890", mysql.TypeDatetime, 4), mustNewTimeDatum("0939-03-21 14:01:42.5772", mysql.TypeDatetime, 4)},
		{mustNewTimeDatum("2021-10-19 12:34:56.7890", mysql.TypeTimestamp, 4), mustNewTimeDatum("2019-03-21 14:01:42.5772", mysql.TypeTimestamp, 4)},
	}

	for _, test := range tests {
		to, _, err := WorkloadSimMask(test.from, nil)
		require.Nil(t, err)
		require.Equal(t, test.expected.String(), to.String()) // fixme: cannot expect the bit representation to be exactly same now
	}
}
