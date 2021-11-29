package main

import (
	"path/filepath"
	"sync"

	"github.com/BugenZhao/sql-masker/tidb"
)

var (
	globalInstance     *tidb.Instance
	globalInstanceOnce sync.Once
)

// Create a new `tidb.Instance`, then run all sqls under `DDLDir` to build schema,
// should be run only once
func initializeDB() error {
	var err error
	globalInstance, err = tidb.NewInstance()
	if err != nil {
		return err
	}

	db, err := globalInstance.OpenContext()
	if err != nil {
		return err
	}

	err = db.MayCreateDB(globalOption.DB)
	if err != nil {
		return err
	}
	err = db.UseDB(globalOption.DB)
	if err != nil {
		return err
	}

	for _, dir := range globalOption.DDLDir {
		ddls := make(chan string)
		paths, _ := filepath.Glob(dir + "/*.sql")
		go ReadSQLs(ddls, paths...)
		for sql := range ddls {
			if globalOption.FilterOutConstraints {
				if globalOption.IgnoreIntPK {
					// ignore masking int pk means we should KEEP INFO of int pk
					err = db.ExecuteWithTransform(sql, filterOutConstraintsKeepIntPKInfo)
				} else {
					err = db.ExecuteWithTransform(sql, filterOutAllConstraints)
				}
			} else {
				err = db.Execute(sql)
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Create a new `tidb.Context` (may initialize a `tidb.Instance` first),
// then run all sqls under `PrepareDir` for this session
func NewPreparedTiDBContext() (*tidb.Context, error) {
	globalInstanceOnce.Do(func() {
		err := initializeDB()
		if err != nil {
			panic(err)
		}
	})

	db, err := globalInstance.OpenContext()
	if err != nil {
		return nil, err
	}
	_ = db.UseDB(globalOption.DB)

	for _, dir := range globalOption.PrepareDir {
		ddls := make(chan string)
		paths, _ := filepath.Glob(dir + "/*.sql")
		go ReadSQLs(ddls, paths...)
		for sql := range ddls {
			err = db.Execute(sql)
			if err != nil {
				return nil, err
			}
		}
	}

	return db, nil
}
