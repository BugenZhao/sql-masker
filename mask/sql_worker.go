package mask

import "github.com/BugenZhao/sql-masker/tidb"

type SQLWorker struct {
	worker
}

func NewSQLWorker(db *tidb.Instance, maskFunc MaskFunc) *SQLWorker {
	return &SQLWorker{
		worker: *newWorker(db, maskFunc),
	}
}

func (w *SQLWorker) MaskOne(sql string) (string, error) {
	w.Stats.All += 1

	newSQL, err := w.maskOneQuery(sql)
	if err == nil {
		w.Stats.Success += 1
	} else if newSQL != sql {
		w.Stats.Problematic += 1
	}

	return newSQL, err
}
