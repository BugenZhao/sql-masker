package main

import (
	"github.com/BugenZhao/sql-masker/tidb"
	"github.com/jpillora/opts"
)

type Option struct {
	SQLOption   `opts:"mode=cmd, name=sql,   help=Mask SQL queries"`
	EventOption `opts:"mode=cmd, name=event, help=Mask MySQL events"`
	DDLFile     string `opts:"name=ddl"`
}

var option *Option

func main() {
	option = &Option{}
	opts.Parse(option).RunFatal()
}

func NewDefinedInstance() (*tidb.Instance, error) {
	db, err := tidb.NewInstance()
	if err != nil {
		return nil, err
	}

	if option.DDLFile != "" {
		ddls := make(chan string)
		go ReadSQLs(option.DDLFile, ddls)
		for sql := range ddls {
			err = db.Execute(sql)
			if err != nil {
				return nil, err
			}
		}
	}

	return db, nil
}
