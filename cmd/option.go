package main

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/BugenZhao/sql-masker/mask"
)

type Option struct {
	SQLOption            `opts:"mode=cmd, name=sql,   help=Mask SQL queries"`
	EventOption          `opts:"mode=cmd, name=event, help=Mask MySQL events"`
	DDLDir               []string `opts:"help=directories to DDL SQL files executed only once"`
	PrepareDir           []string `opts:"help=directories to SQL files executed per session"`
	DB                   string   `opts:"help=default database to use"`
	FilterOutConstraints bool     `opts:"help=whether to filter out table constraints for DDL for better type inference"`
	Mask                 string   `opts:"help=name of the mask function"`
}

var globalOption = &Option{
	EventOption: EventOption{
		Concurrency: runtime.NumCPU(),
	},
	DB:                   "test",
	FilterOutConstraints: true,
	Mask:                 "debug",
}

func (o *Option) resolveMaskFunc() mask.MaskFunc {
	fn, ok := mask.MaskFuncMap[strings.ToLower(o.Mask)]
	if !ok {
		keys := make([]string, 0, len(mask.MaskFuncMap))
		for k := range mask.MaskFuncMap {
			keys = append(keys, k)
		}
		panic(fmt.Errorf("no such mask function `%s`, available functions are `%v`", o.Mask, keys))
	}
	return fn
}
