package main

import (
	"github.com/jpillora/opts"
)

type Option struct {
	SQLOption   `opts:"mode=cmd, name=sql,   help=Mask SQL queries"`
	EventOption `opts:"mode=cmd, name=event, help=Mask MySQL events"`
}

func main() {
	option := &Option{
		SQLOption: SQLOption{Dir: "example/min"},
	}
	err := opts.Parse(option).Run()

	if err != nil {
		panic(err)
	}
}
