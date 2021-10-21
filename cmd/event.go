package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/Jeffail/tunny"
	"github.com/zyguan/mysql-replay/event"
	"go.uber.org/zap"
)

type EventOption struct {
	Concurrency int    `opts:"short=t, help=goroutine concurrency for masking, default=CPU nums"`
	InputDir    string `opts:"help=directory to the original event tsvs"`
	OutputDir   string `opts:"help=directory to the masked event tsvs"`
}

func (opt *EventOption) outPath(from string) string {
	return filepath.Join(opt.OutputDir, filepath.Base(from))
}

func (opt *EventOption) RunFile(path string) (*mask.Stats, error) {
	maskFunc := globalOption.resolveMaskFunc()

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
	masker := mask.NewEventWorker(db, maskFunc)

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
			if globalOption.Verbose {
				zap.S().Warnw("failed to mask event", "original", strings.ReplaceAll(text, "\t", " "), "error", err)
			}
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

	pool := tunny.NewFunc(opt.Concurrency, func(arg interface{}) interface{} {
		defer wg.Done()
		path := arg.(string)
		stats, err := opt.RunFile(path)
		resultChan <- TaskResult{
			from:  path,
			to:    opt.outPath(path),
			stats: stats,
			err:   err,
		}
		return nil
	})
	defer pool.Close()

	zap.S().Infow("start masking events...")
	for _, path := range paths {
		wg.Add(1)
		go pool.Process(path)
	}

	go func() {
		wg.Wait()
		time.Sleep(200 * time.Millisecond)
		close(resultChan)
	}()

	i := 1
	all := len(paths)
	stats := mask.Stats{}
	startTime := time.Now()
	for result := range resultChan {
		progress := fmt.Sprintf("%d/%d", i, all)
		if result.err != nil {
			zap.S().Warnw("mask error", "progress", progress, "file", result.from, "error", result.err)
		} else {
			zap.S().Infow("mask done", "progress", progress, "from", result.from, "to", result.to, "stats", result.stats.String())
		}
		i += 1
		stats.Merge(*result.stats)
	}

	zap.S().Infow("all done", "files", all, "stats", stats, "time", time.Since(startTime).String())
	return nil
}
