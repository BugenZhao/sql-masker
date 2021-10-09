package mask

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

type MaskFunc = func(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error)

var (
	_ MaskFunc = IdenticalMask
	_ MaskFunc = DebugMask
	_ MaskFunc = WorkloadSimMask
)

func IdenticalMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	return datum, tp, nil
}

func DebugMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	tpDesc := strings.Split(tp.String(), " ")[0]
	datumDesc, err := datum.ToString()
	if err != nil {
		datumDesc = datum.String()
	}
	info := fmt.Sprintf("%s %s", color.YellowString(tpDesc), color.CyanString(datumDesc))
	return types.NewDatum(info), types.NewFieldType(mysql.TypeString), nil
}

func WorkloadSimMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	panic("unimplemented")
}
