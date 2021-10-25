package mask

import "github.com/BugenZhao/sql-masker/tidb"

type SQLWorker struct {
	worker
}

func NewSQLWorker(db *tidb.Context, maskFunc MaskFunc, ignoreIntPK bool) *SQLWorker {
	return &SQLWorker{
		worker: *newWorker(db, maskFunc, ignoreIntPK),
	}
}

func (w *SQLWorker) MaskOne(sql string) (string, error) {
	w.Stats.All += 1

	newSQL, err := w.maskOneQuery(sql)
	if err != nil {
		if newSQL != "" { // problematic
			w.Stats.Problematic += 1
		}
		return newSQL, err
	}

	w.Stats.Success += 1
	return newSQL, nil
}
