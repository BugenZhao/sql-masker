package main

import (
	"fmt"

	"github.com/BugenZhao/sql-masker/mask"
	maskfuncs "github.com/BugenZhao/sql-masker/mask/funcs"
	"github.com/fatih/color"
)

type SQLOption struct {
	File string `opts:"help=SQL file to mask"`
}

func (opt *SQLOption) Run() error {
	db, err := NewPreparedTiDBContext()
	if err != nil {
		return err
	}

	masker := mask.NewSQLWorker(db, maskfuncs.DebugMaskColor)
	maskSQLs := make(chan string)
	go ReadSQLs(maskSQLs, opt.File)
	for sql := range maskSQLs {
		fmt.Printf("\n-> %s\n", sql)
		newSQL, err := masker.MaskOne(sql)
		if err != nil {
			if newSQL == "" || newSQL == sql {
				color.Red("!> %v\n", err)
				continue
			} else {
				color.Yellow("?> %v\n", err)
			}
		}
		fmt.Printf("=> %s\n", newSQL)
	}

	masker.Stats.PrintSummary()
	return nil
}
