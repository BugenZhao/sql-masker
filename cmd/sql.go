package main

import (
	"fmt"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/fatih/color"
)

type SQLOption struct {
	File string `opts:"help=file"`
}

func (opt *SQLOption) Run() error {
	db, err := NewDefinedInstance()
	if err != nil {
		return err
	}

	masker := mask.NewWorker(db, mask.DebugMaskColor)
	maskSQLs := make(chan string)
	go ReadSQLs(opt.File, maskSQLs)
	for sql := range maskSQLs {
		fmt.Printf("\n-> %s\n", sql)
		newSQL, err := masker.MaskOneQuery(sql)
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

	fmt.Printf(`

====Summary====
Success      %d
Problematic  %d
Failed       %d
Total        %d
`,
		masker.Success, masker.Problematic, masker.All-masker.Success-masker.Problematic, masker.All)
	return nil
}
