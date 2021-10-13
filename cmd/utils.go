package main

import (
	"bufio"
	"io"
	"os"
	"strings"
)

func ReadSQLs(out chan<- string, sqlPaths ...string) {
	defer close(out)

	for _, path := range sqlPaths {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		reader := bufio.NewReader(file)
		for {
			sql, err := reader.ReadString(';')
			if err == io.EOF {
				break
			} else if err != nil {
				return
			}
			out <- strings.TrimSpace(sql)
		}
	}
}
