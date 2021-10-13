package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/fatih/color"
	"github.com/zyguan/mysql-replay/event"
)

type EventOption struct {
	File string `opts:"help=file"`
}

func (opt *EventOption) Run() error {
	file, err := os.Open(opt.File)
	if err != nil {
		return err
	}
	defer file.Close()
	in := bufio.NewScanner(file)

	db, err := NewDefinedInstance()
	if err != nil {
		return err
	}
	masker := mask.NewWorker(db, mask.DebugMask)

	for in.Scan() {
		ev := event.MySQLEvent{}
		text := in.Text()
		_, err := event.ScanEvent(text, 0, &ev)
		if err != nil {
			return err
		}

		fmt.Printf("\n-> %s\n", text)
		ev, err = masker.MaskEvent(ev)
		if err != nil {
			color.Red("!> %v\n", err)
			continue
		}

		maskedText := []byte{}
		maskedText, err = event.AppendEvent(maskedText, ev)
		if err != nil {
			color.Red("!> %v\n", err)
			continue
		}
		fmt.Printf("=> %s\n", maskedText)
	}

	return nil
}
