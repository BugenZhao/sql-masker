package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/BugenZhao/sql-masker/mask"
	maskfuncs "github.com/BugenZhao/sql-masker/mask/funcs"
	"github.com/zyguan/mysql-replay/event"
	"go.uber.org/zap"
)

type EventOption struct {
	InputDir  string `opts:""`
	OutputDir string `opts:""`
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
	masker := mask.NewEventWorker(db, maskfuncs.WorkloadSimMask)

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
	defer out.Flush()

	for in.Scan() {
		ev := event.MySQLEvent{}
		text := in.Text()
		_, err := event.ScanEvent(text, 0, &ev)
		if err != nil {
			return nil, err
		}

		mev, err := masker.MaskOne(ev)
		if err != nil {
			zap.S().Warnw("failed to mask event", "original", strings.ReplaceAll(text, "\t", " "), "error", err)
			mev = ev
		}

		maskedLine := []byte{}
		maskedLine, err = event.AppendEvent(maskedLine, mev)
		if err != nil {
			return nil, err
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
				from:  path,
				to:    opt.outPath(path),
				stats: stats,
				err:   err,
			}
		}(path)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	i := 1
	all := len(paths)
	for result := range resultChan {
		progress := fmt.Sprintf("%d/%d", i, all)
		if result.err != nil {
			zap.S().Warnw("mask error", "progress", progress, "file", result.from, "error", result.err)
		} else {
			zap.S().Infow("mask done", "progress", progress, "from", result.from, "to", result.to, "stats", result.stats.String())
		}
		i += 1
	}

	zap.S().Infow("all done")
	return nil
}
