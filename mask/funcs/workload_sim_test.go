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

		{types.NewFloat64Datum(42.42), types.NewFloat64Datum(7818787403329284e135)},
		{types.NewFloat32Datum(42.42), types.NewFloat32Datum(9.713432e36)},
		{types.NewStringDatum("你好"), types.NewStringDatum("ba3468")},
		{types.NewBytesDatum([]byte("\x01\x02")), types.NewBytesDatum([]byte("0a"))},
		{types.NewMysqlEnumDatum(types.Enum{Name: "male", Value: 1}), types.NewMysqlEnumDatum(types.Enum{Name: "d85e", Value: 1})},

		{mustNewDurationDatum("10:11:12.1314", 4), mustNewDurationDatum("10:00:00.0000", 4)},
		{mustNewTimeDatum("2021-10-19", mysql.TypeDate, 0), mustNewTimeDatum("2021-10-19", mysql.TypeDate, 0)},
		{mustNewTimeDatum("2021-10-19 12:34:56.7890", mysql.TypeDatetime, 4), mustNewTimeDatum("2021-10-19 00:00:00.0000", mysql.TypeDatetime, 4)},
		{mustNewTimeDatum("2021-10-19 12:34:56.7890", mysql.TypeTimestamp, 4), mustNewTimeDatum("2021-10-19 00:00:00.0000", mysql.TypeTimestamp, 4)},
	}

	for _, test := range tests {
		to, _, err := WorkloadSimMask(test.from, nil)
		require.Nil(t, err)
		require.Equal(t, test.expected, to)
	}
}
