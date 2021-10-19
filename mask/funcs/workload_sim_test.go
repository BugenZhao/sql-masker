package funcs

import (
	"testing"

	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestWorkloadSimMask(t *testing.T) {
	t.Parallel()

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
		{types.NewStringDatum("你好"), types.NewStringDatum("ba3468")},
		{types.NewBytesDatum([]byte("\x01\x02")), types.NewBytesDatum([]byte("0a"))},
	}

	for _, test := range tests {
		to, _, err := WorkloadSimMask(test.from, nil)
		require.Nil(t, err)
		require.Equal(t, test.expected, to)
	}
}
