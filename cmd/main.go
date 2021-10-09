package main

import (
	"fmt"

	"github.com/BugenZhao/sql-masker/tidb"
)

func main() {
	err := play()
	if err != nil {
		panic(err)
	}
}

func play() error {
	db, err := tidb.NewTiDBInstance()
	if err != nil {
		return err
	}

	executeSQLs := []string{
		"use test;",
		"create table test.t(id int, name varchar(24), birth datetime, deci decimal(6,2));",
		"insert into test.t values (1, '233', '2021-09-30 12:34:56', 12.34);",
	}

	for _, sql := range executeSQLs {
		_, err = db.Execute(sql)
		if err != nil {
			return err
		}
	}

	inferSQLs := []string{
		// "select * from t where name = 233;", // table reader
		"select * from t where birth between 2021 and '2022'",
		"select * from t where birth <= 2010 and birth >= 2000",
		"select * from t where name = 233 and birth = 233 and id = 233",
		// "select * from t where name between 200 and 300;",
		"select * from t where deci >= 1.234e2",
		"select * from t where id = '23';", // point get
		// "select * from t where id = '23' and name = 233;", // selection with child (point get)
	}

	inferer := tidb.NewInferer(db)

	for _, sql := range inferSQLs {
		fmt.Println()
		fmt.Println(sql)

		err := inferer.Infer(sql)
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}
