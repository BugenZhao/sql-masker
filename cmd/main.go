package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/BugenZhao/sql-masker/tidb"
)

func main() {
	err := play()
	if err != nil {
		panic(err)
	}
}

func play() error {
	db, err := tidb.NewInstance()
	if err != nil {
		return err
	}

	executeSQLs := make(chan string)
	go readSQLs("example/execute.sql", executeSQLs)
	for sql := range executeSQLs {
		err = db.Execute(sql)
		if err != nil {
			return err
		}
	}

	masker := mask.NewWorker(db, mask.DebugMask)
	maskSQLs := make(chan string)
	go readSQLs("example/mask.sql", maskSQLs)
	for sql := range maskSQLs {
		fmt.Println()
		fmt.Println(sql)
		newSQL, err := masker.MaskOne(sql)
		if err != nil {
			fmt.Printf("FAILED: %v\n", err)
		} else {
			fmt.Println(newSQL)
		}
	}

	fmt.Printf("\n\n===Summary===\nSuccess %d/%d\n", masker.Success, masker.All)
	return nil
}

func readSQLs(path string, sqlOut chan<- string) {
	defer close(sqlOut)
	file, err := os.Open(path)
	if err != nil {
		return
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
		sqlOut <- sql
	}
}
