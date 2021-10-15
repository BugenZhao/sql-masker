package main

import "runtime"

type Option struct {
	SQLOption            `opts:"mode=cmd, name=sql,   help=Mask SQL queries"`
	EventOption          `opts:"mode=cmd, name=event, help=Mask MySQL events"`
	DDLDir               []string `opts:"help=directories to DDL SQL files executed only once"`
	PrepareDir           []string `opts:"help=directories to SQL files executed per session"`
	DB                   string   `opts:"help=default database to use"`
	FilterOutConstraints bool     `opts:"help=whether to filter out table constraints for DDL for better type inference"`
}

var globalOption = &Option{
	EventOption: EventOption{
		Concurrency: runtime.NumCPU(),
	},
	DB:                   "test",
	FilterOutConstraints: true,
}
