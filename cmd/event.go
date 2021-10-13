package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/fatih/color"
	"github.com/zyguan/mysql-replay/event"
)

type EventOption struct {
	InputDir  string `opts:""`
	OutputDir string `opts:""`
	Verbose   bool   `opt:""`
}

func (opt *EventOption) outPath(from string) string {
	return filepath.Join(opt.OutputDir, filepath.Base(from))
}

func (opt *EventOption) RunFile(path string) (*mask.Stats, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	in := bufio.NewScanner(file)

	db, err := NewPreparedTiDBContext()
	if err != nil {
		return nil, err
	}
	masker := mask.NewEventWorker(db, mask.DebugMask)

	outPath := opt.outPath(file.Name())
	if _, err := os.Stat(outPath); err == nil {
		return nil, fmt.Errorf("file %s already exists", outPath)
	}
	outFile, err := os.Create(outPath)
	if err != nil {
		return nil, err
	}
	defer outFile.Close()
	out := bufio.NewWriter(outFile)

	for in.Scan() {
		ev := event.MySQLEvent{}
		text := in.Text()
		_, err := event.ScanEvent(text, 0, &ev)
		if err != nil {
			return nil, err
		}

		ev, err = masker.MaskOne(ev)
		if err != nil {
			if opt.Verbose {
				fmt.Printf("\n-> %s\n", text)
				color.Red("!> %v\n", err)
			}
			continue
		}

		maskedLine := []byte{}
		maskedLine, err = event.AppendEvent(maskedLine, ev)
		if err != nil {
			if opt.Verbose {
				color.Red("!> %v\n", err)
			}
		}

		maskedLine = append(maskedLine, '\n')
		_, err = out.Write(maskedLine)
		if err != nil {
			return nil, err
		}
	}

	return &masker.Stats, nil
}

func (opt *EventOption) Run() error {
	if opt.OutputDir == "" {
		return fmt.Errorf("output dir not given")
	}
	err := os.MkdirAll(opt.OutputDir, os.ModePerm)
	if err != nil {
		return err
	}

	paths, _ := filepath.Glob(opt.InputDir + "/*")
	wg := new(sync.WaitGroup)
	resultChan := make(chan TaskResult)

	for _, path := range paths {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			stats, err := opt.RunFile(path)
			resultChan <- TaskResult{
				opt.outPath(path),
				stats,
				err,
			}
		}(path)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for result := range resultChan {
		fmt.Printf("\n%s:", result.file)
		if result.err != nil {
			color.Red("\n%v", result.err)
		} else {
			result.stats.Summary()
		}
	}

	return nil
}
