package main

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/BugenZhao/sql-masker/tidb"
	"github.com/jpillora/opts"
)

type Option struct {
	SQLOption            `opts:"mode=cmd, name=sql,   help=Mask SQL queries"`
	EventOption          `opts:"mode=cmd, name=event, help=Mask MySQL events"`
	DDLDir               []string `opts:"help=directories to DDL SQL files"`
	DB                   string   `opts:"help=default database to use"`
	FilterOutConstraints bool     `opts:"help=whether to filter out table constraints for DDL"`
}

var option *Option

func main() {
	option = &Option{
		DB:                   "test",
		FilterOutConstraints: true,
	}
	opts.Parse(option).RunFatal()
}

var (
	globalInstance     *tidb.Instance
	globalInstanceOnce sync.Once
)

func prepareDB() error {
	var err error
	globalInstance, err = tidb.NewInstance()
	if err != nil {
		return err
	}

	db, err := globalInstance.OpenContext()
	if err != nil {
		return err
	}

	db.Execute(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", option.DB))
	db.Execute(fmt.Sprintf("USE `%s`", option.DB))

	for _, dir := range option.DDLDir {
		ddls := make(chan string)
		paths, _ := filepath.Glob(dir + "/*.sql")
		go ReadSQLs(ddls, paths...)
		for sql := range ddls {
			if option.FilterOutConstraints {
				err = db.ExecuteWithTransform(sql, filterOutConstraints)
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

func NewDefinedTiDBContext() (*tidb.Context, error) {
	globalInstanceOnce.Do(func() {
		err := prepareDB()
		if err != nil {
			panic(err)
		}
	})

	db, err := globalInstance.OpenContext()
	if err != nil {
		return nil, err
	}
	db.Execute(fmt.Sprintf("USE `%s`", option.DB))

	return db, nil
}
