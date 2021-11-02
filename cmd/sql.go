package main

import (
	"fmt"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/fatih/color"
)

type SQLOption struct {
	File string `opts:"help=SQL file to mask"`
}

func (opt *SQLOption) Run() error {
	maskFunc := globalOption.ResolveMaskFunc()

	db, err := NewPreparedTiDBContext()
	if err != nil {
		return err
	}

	nameMap := globalOption.ReadNameMap()
	masker := mask.NewSQLWorker(db, maskFunc, globalOption.IgnoreIntPK, nameMap)

	maskSQLs := make(chan string)
	go ReadSQLs(maskSQLs, opt.File)
	for sql := range maskSQLs {
		fmt.Printf("\n-> %s\n", sql)
		newSQL, err := masker.MaskOne(sql)
		if err != nil {
			if newSQL != "" { // problematic
				color.Yellow("?> %v\n", err)
			} else {
				color.Red("!> %v\n", err)
				continue
			}
		}
		fmt.Printf("=> %s\n", newSQL)
	}

	masker.Stats.PrintSummary()
	return nil
}
