package mask

import "github.com/pingcap/tidb/types"

type MaskFunc = func(datum types.Datum, tp *types.FieldType) (types.Datum, error)

var (
	_ MaskFunc = IdenticalMask
)

func IdenticalMask(datum types.Datum, tp *types.FieldType) (types.Datum, error) {
	return datum, nil
}
