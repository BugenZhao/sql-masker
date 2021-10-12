package main

import (
	"bufio"
	"io"
	"os"
	"strings"
)

func ReadSQLs(path string, sqlOut chan<- string) {
	defer close(sqlOut)
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
		sqlOut <- strings.TrimSpace(sql)
	}
}
