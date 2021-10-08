package tidb

import "github.com/pingcap/tidb/types"


func EvalTypeToString(t types.EvalType) string {
	switch t {
	case types.ETInt:
		return "Int"
	case types.ETReal:
		return "Real"
	case types.ETDecimal:
		return "Decimal"
	case types.ETString:
		return "String"
	case types.ETDatetime:
		return "Datetime"
	case types.ETTimestamp:
		return "Timestamp"
	case types.ETDuration:
		return "Duration"
	case types.ETJson:
		return "Json"
	default:
		return ""
	}
}
