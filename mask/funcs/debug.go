package funcs

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/pingcap/tidb/types"
)

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
	return types.NewDatum(info), stringTp, nil
}

func DebugMask(datum types.Datum, tp *types.FieldType) (types.Datum, *types.FieldType, error) {
	tpDesc, datumDesc := extractDebugInfo(datum, tp)
	info := fmt.Sprintf("%s %s", tpDesc, datumDesc)
	return types.NewDatum(info), stringTp, nil
}
