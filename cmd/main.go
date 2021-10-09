package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/BugenZhao/sql-masker/tidb"
	"github.com/jpillora/opts"
)

type Opt struct {
	Dir string `opts:"help=directory to SQLs"`
}

func main() {
	opt := &Opt{
		Dir: "example/min",
	}
	opts.Parse(opt)

	err := play(opt)
	if err != nil {
		panic(err)
	}
}

func play(opt *Opt) error {
	db, err := tidb.NewInstance()
	if err != nil {
		return err
	}

	executeSQLs := make(chan string)
	go readSQLs(opt.Dir+"/execute.sql", executeSQLs)
	for sql := range executeSQLs {
		err = db.Execute(sql)
		if err != nil {
			return err
		}
	}

	masker := mask.NewWorker(db, mask.DebugMask)
	maskSQLs := make(chan string)
	go readSQLs(opt.Dir+"/mask.sql", maskSQLs)
	for sql := range maskSQLs {
		fmt.Printf("\n-> %s\n", sql)
		newSQL, err := masker.MaskOne(sql)
		if err != nil {
			if newSQL == "" || newSQL == sql {
				fmt.Printf("!> %v\n", err)
				continue
			} else {
				fmt.Printf("?> %v\n", err)
			}
		}
		fmt.Printf("=> %s\n", newSQL)
	}

	fmt.Printf(`

====Summary====
Success      %d
Problematic  %d
Total        %d
`,
		masker.Success, masker.Problematic, masker.All)
	return nil
}

func readSQLs(path string, sqlOut chan<- string) {
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
