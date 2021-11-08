package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/BugenZhao/sql-masker/mask"
)

type Option struct {
	SQLOption            `opts:"mode=cmd, name=sql,   help=Mask SQL queries"`
	EventOption          `opts:"mode=cmd, name=event, help=Mask MySQL events"`
	ListOption           `opts:"mode=cmd, name=list,  help=List all mask functions"`
	NameOption           `opts:"mode=cmd, name=name,  help=Generate name maps"`
	DDLDir               []string `opts:"help=directories to DDL SQL files executed only once"`
	PrepareDir           []string `opts:"help=directories to SQL files executed per session"`
	DB                   string   `opts:"help=default database to use"`
	FilterOutConstraints bool     `opts:"help=whether to filter out table constraints for DDL for better type inference"`
	IgnoreIntPK          bool     `opts:"help=whether to ignore masking for integer primary keys"`
	Mask                 string   `opts:"help=name of the mask function"`
	Verbose              bool     `opts:"help=whether to print warnings for failed entry"`
	NameMapPath          string   `opts:"name=name-map, help=path to name map"`
}

var globalOption = &Option{
	EventOption: EventOption{
		Concurrency: runtime.NumCPU(),
	},
	NameOption: NameOption{
		MaskedDBPrefix: "_mdb",
	},
	DB:                   "test",
	FilterOutConstraints: true,
	Mask:                 "debug",
}

func (o *Option) ResolveMaskFunc() mask.MaskFunc {
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

var (
	nameMap     mask.NameMap
	nameMapOnce sync.Once
)

func (o *Option) ReadNameMap() *mask.NameMap {
	nameMapOnce.Do(func() {
		if o.NameMapPath == "" {
			return
		}

		bytes, err := os.ReadFile(o.NameMapPath)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(bytes, &nameMap)
		if err != nil {
			panic(fmt.Errorf("bad name map format; %w", err))
		}
	})

	if len(nameMap.Columns) == 0 {
		return nil
	} else {
		return &nameMap
	}
}
