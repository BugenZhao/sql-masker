package funcs

import "github.com/pingcap/tidb/types"

func IdenticalMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	return datum, tp, nil
}
