package mask

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

type MaskFunc = func(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error)

var (
	_ MaskFunc = IdenticalMask
	_ MaskFunc = DebugMask
)

func IdenticalMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	return datum, tp, nil
}

func DebugMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	return types.NewDatum(datum.String()), types.NewFieldType(mysql.TypeString), nil
}
