package mask

import (
	"fmt"

	"github.com/BugenZhao/sql-masker/tidb"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/zyguan/mysql-replay/event"
)

type Prepared struct {
	sql           string
	typeMap       TypeMap
	sortedMarkers []ReplaceMarker
}

type PreparedMap = map[uint64]Prepared

type EventWorker struct {
	worker
	preparedStmts PreparedMap
}

// Create a mask worker for MySQL Events
func NewEventWorker(db *tidb.Context, maskFunc MaskFunc, ignoreIntPK bool, nameMap *NameMap) *EventWorker {
	return &EventWorker{
		worker:        *newWorker(db, maskFunc, ignoreIntPK, nameMap),
		preparedStmts: make(PreparedMap),
	}
}

// For type `StmtPrepare`, do not evaluate but only analyze it and store into `w.preparedStmts`
func (w *EventWorker) PrepareOne(stmtID uint64, sql string) (string, error) {
	replacedStmtNode, sortedMarkers, err := w.replaceParamMarker(sql)
	if err != nil {
		return "", err
	}
	inferredTypes, localNameMap, err := w.infer(replacedStmtNode)
	if err != nil {
		return "", err
	}

	newSQL := sql
	if localNameMap != nil {
		stmtNode, err := w.db.ParseOne(sql)
		if err != nil {
			return "", err
		}
		v := NewNameOnlyRestoreVisitor(localNameMap)

		newNode, ok := stmtNode.Accept(v)
		if !ok {
			return "", v.err
		}
		newSQL, err = w.db.RestoreSQL(newNode)
		if err != nil {
			return "", err
		}
	}

	w.preparedStmts[stmtID] = Prepared{
		sql, inferredTypes, sortedMarkers,
	}
	return newSQL, nil
}

// For type `StmtExecute`, lookup prepared analysis from `w.preparedStmts` and mask parameters
func (w *EventWorker) MaskOneExecute(stmtID uint64, params []interface{}) ([]interface{}, error) {
	p, ok := w.preparedStmts[stmtID]
	if !ok {
		return params, fmt.Errorf("no prepared query found for stmt id `%d`", stmtID)
	}

	if len(p.sortedMarkers) != len(params) {
		return params, fmt.Errorf("mismatched length of inferred markers and params for stmt `%s`", p.sql)
	}

	sc := &stmtctx.StatementContext{}
	maskedParams := []interface{}{}
	var err error

	for i, param := range params {
		originDatum := types.NewDatum(param)

		marker := p.sortedMarkers[i]
		// HACK: Most of constants in `PREPARE` statements are not considered to be masked.
		//       In replace phase, we have replaced all constants with `1`, so this may help
		//       to handle expressions like `? +/-/*/div {constant}`
		possibleMarkers := []ReplaceMarker{
			marker,
			marker + 1,
			marker - 1,
		}

		var tp *InferredType
		for _, marker := range possibleMarkers {
			tp, ok = p.typeMap[marker]
			if ok {
				break
			}
		}
		if tp == nil {
			err = fmt.Errorf("type for `%v` not inferred; %w", originDatum, err)
			maskedParams = append(maskedParams, originDatum)
			continue
		}

		var maskedDatum types.Datum
		if tp.IsPrimaryKey() && w.ignoreIntPK {
			// use original datum if int pk is ignored
			maskedDatum = originDatum
		} else {
			maskedDatum, _, err = ConvertAndMask(sc, originDatum, tp.Ft, w.maskFunc)
			if err != nil {
				return params, err
			}
		}

		maskedParam := datumToEventParam(maskedDatum)
		maskedParams = append(maskedParams, maskedParam)
	}

	return maskedParams, nil
}

func (w *EventWorker) MaskOne(ev event.MySQLEvent) (event.MySQLEvent, error) {
	w.Stats.All += 1

	switch ev.Type {
	case event.EventHandshake:
		w.preparedStmts = make(PreparedMap)
		w.db.UseDB(ev.DB)
		if w.globalNameMap != nil {
			ev.DB = w.globalNameMap.DB(ev.DB)
		}

	case event.EventQuery:
		maskedQuery, err := w.maskOneQuery(ev.Query)
		if err != nil {
			if maskedQuery != "" { // problematic
				ev.Query = maskedQuery
				err = nil
				w.Stats.Problematic += 1
			} else {
				ev.Query = fmt.Sprintf("/* FAILED: %v */ %s", err, ev.Query)
			}
			return ev, err
		}
		ev.Query = maskedQuery

	case event.EventStmtPrepare:
		newSQL, err := w.PrepareOne(ev.StmtID, ev.Query)
		if err != nil {
			return ev, err
		}
		ev.Query = newSQL

	case event.EventStmtExecute:
		maskedParams, err := w.MaskOneExecute(ev.StmtID, ev.Params)
		if err != nil {
			return ev, err
		}
		ev.Params = maskedParams

	case event.EventStmtClose:
		delete(w.preparedStmts, ev.StmtID)

	default:
	}

	w.Stats.Success += 1
	return ev, nil
}

func datumToEventParam(datum types.Datum) interface{} {
	/*
		case KindMysqlDecimal:
			return d.GetMysqlDecimal()
		case KindMysqlDuration:
			return d.GetMysqlDuration()
		case KindMysqlEnum:
			return d.GetMysqlEnum()
		case KindBinaryLiteral, KindMysqlBit:
			return d.GetBinaryLiteral()
		case KindMysqlSet:
			return d.GetMysqlSet()
		case KindMysqlJSON:
			return d.GetMysqlJSON()
		case KindMysqlTime:
			return d.GetMysqlTime()
		default:
			return d.GetInterface()
	*/

	switch value := datum.GetValue().(type) {
	case int64, uint64, string, float32, float64, []byte:
		return value

	case *types.MyDecimal:
		f, _ := value.ToFloat64()
		return f

	case types.Duration:
		return value.String()
	case types.Time:
		return value.String()

	default:
		return value
	}
}
