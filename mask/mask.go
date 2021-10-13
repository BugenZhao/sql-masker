package mask

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type MaskFunc = func(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error)

func ConvertAndMask(sc *stmtctx.StatementContext, datum types.Datum, toType *types.FieldType, maskFunc MaskFunc) (types.Datum, *types.FieldType, error) {
	castedDatum, err := datum.ConvertTo(sc, toType)
	if err != nil {
		return datum, nil, fmt.Errorf("cannot cast `%v` to type `%v`; %w", datum, toType, err)
	}

	maskedDatum, maskedType, err := maskFunc(castedDatum, toType)
	if err != nil {
		return castedDatum, toType, fmt.Errorf("failed to mask `%v`; %w", castedDatum, err)
	}

	if maskedType == nil {
		maskedType = toType
	}
	return maskedDatum, toType, nil
}

var (
	_ MaskFunc = IdenticalMask
	_ MaskFunc = DebugMask
	_ MaskFunc = WorkloadSimMask
)

func IdenticalMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	return datum, tp, nil
}

func extractDebugInfo(datum types.Datum, tp *types.FieldType) (tpDesc string, datumDesc string) {
	tpDesc = strings.Split(tp.String(), " ")[0]
	datumDesc, err := datum.ToString()
	if err != nil {
		datumDesc = datum.String()
	}
	return
}

func DebugMaskColor(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	tpDesc, datumDesc := extractDebugInfo(datum, tp)
	info := fmt.Sprintf("%s %s", color.GreenString(tpDesc), color.CyanString(datumDesc))
	return types.NewDatum(info), types.NewFieldType(mysql.TypeString), nil
}

func DebugMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	tpDesc, datumDesc := extractDebugInfo(datum, tp)
	info := fmt.Sprintf("%s %s", tpDesc, datumDesc)
	return types.NewDatum(info), types.NewFieldType(mysql.TypeString), nil
}

func WorkloadSimMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	panic("unimplemented")
}
