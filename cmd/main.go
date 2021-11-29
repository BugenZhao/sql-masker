package main

import (
	"github.com/jpillora/opts"
)

func main() {
	initLogger()
	opts.Parse(globalOption).RunFatal()
}
