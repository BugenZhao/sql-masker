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

func NewEventWorker(db *tidb.Context, maskFunc MaskFunc) *EventWorker {
	return &EventWorker{
		worker:        *newWorker(db, maskFunc),
		preparedStmts: make(PreparedMap),
	}
}

func (w *EventWorker) PrepareOne(stmtID uint64, sql string) error {
	replacedStmtNode, sortedMarkers, err := w.replaceParamMarker(sql)
	if err != nil {
		return err
	}
	inferredTypes, err := w.infer(replacedStmtNode)
	if err != nil {
		return err
	}

	w.preparedStmts[stmtID] = Prepared{
		sql, inferredTypes, sortedMarkers,
	}
	return nil
}

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
		// HACK: handle `? +/-/*/div {constant}`
		possibleMarkers := []ReplaceMarker{
			marker,
			marker + 1,
			marker - 1,
		}

		var tp *types.FieldType
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

		maskedDatum, _, err := ConvertAndMask(sc, originDatum, tp, w.maskFunc)
		if err != nil {
			return params, err
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

	case event.EventQuery:
		maskedQuery, err := w.maskOneQuery(ev.Query)
		if err != nil {
			if maskedQuery != ev.Query {
				w.Stats.Problematic += 1
			}
			return ev, err
		}
		ev.Query = maskedQuery

	case event.EventStmtPrepare:
		err := w.PrepareOne(ev.StmtID, ev.Query)
		if err != nil {
			return ev, err
		}

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
	case *types.MyDecimal:
		f, _ := value.ToFloat64()
		return f
	case *types.Duration:
		return value.String()
	case *types.Time:
		return value.String()
	default:
		return value
	}
}
