package main

import (
	"fmt"
	"path/filepath"

	"github.com/BugenZhao/sql-masker/tidb"
	"github.com/jpillora/opts"
)

type Option struct {
	SQLOption   `opts:"mode=cmd, name=sql,   help=Mask SQL queries"`
	EventOption `opts:"mode=cmd, name=event, help=Mask MySQL events"`
	DDLDir      []string `opts:""`
	Db          string   `opts:""`
}

var option *Option

func main() {
	option = &Option{
		Db: "test",
	}
	opts.Parse(option).RunFatal()
}

func NewDefinedInstance() (*tidb.Instance, error) {
	db, err := tidb.NewInstance()
	if err != nil {
		return nil, err
	}

	db.Execute(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", option.Db))
	db.Execute(fmt.Sprintf("USE `%s`", option.Db))

	for _, dir := range option.DDLDir {
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
