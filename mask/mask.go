package mask

import (
	"fmt"

	"github.com/BugenZhao/sql-masker/mask/funcs"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type MaskFunc struct {
	Description string
	fn          func(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error)
}

var MaskFuncMap = map[string]MaskFunc{
	"workload-sim": {"For workload simulation project", funcs.WorkloadSimMask},
	"debug":        {"Replace every constant with its inferred type, for debug usage", funcs.DebugMask},
	"debug-color":  {"Like `debug`, but in ANSI color", funcs.DebugMaskColor},
	"identical":    {"Dry-run baseline", funcs.IdenticalMask},
}

func ConvertAndMask(sc *stmtctx.StatementContext, datum types.Datum, toType *types.FieldType, maskFunc MaskFunc) (types.Datum, *types.FieldType, error) {
	castedDatum, err := datum.ConvertTo(sc, toType)
	if err != nil {
		return datum, nil, fmt.Errorf("cannot cast `%v` to type `%v`; %w", datum, toType, err)
	}

	maskedDatum, maskedType, err := maskFunc.fn(*castedDatum.Clone(), toType)
	if err != nil {
		return castedDatum, toType, fmt.Errorf("failed to mask `%v`; %w", castedDatum, err)
	}

	if maskedType == nil {
		maskedType = toType
	}
	return maskedDatum, maskedType, nil
}
