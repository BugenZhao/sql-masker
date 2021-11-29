package main

import (
	"bufio"
	"io"
	"os"
	"strings"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/BugenZhao/sql-masker/tidb"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func initLogger() {
	config := zap.NewProductionConfig()
	config.Encoding = "console"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, _ := config.Build()
	zap.ReplaceGlobals(logger)
}

type TaskResult struct {
	from  string
	to    string
	stats *mask.Stats
	err   error
}

// Read SQL files into statements and output to `out` chan
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

var (
	_ tidb.StmtTransform = filterOutConstraintsKeepIntPKInfo
	_ tidb.StmtTransform = filterOutAllConstraints
)

func filterOutConstraintsKeepIntPKInfo(s ast.StmtNode) ast.StmtNode {
	if s, ok := s.(*ast.CreateTableStmt); ok {
		// todo: check whether required to filter out column options like `primary key`, since we may be able to handle it ?

		intColSet := set.NewStringSet()
		for _, col := range s.Cols {
			if col.Tp.EvalType() == types.ETInt {
				intColSet.Insert(col.Name.Name.L)
			}
		}

		cs := []*ast.Constraint{}
		for _, c := range s.Constraints {
			switch c.Tp {
			case ast.ConstraintPrimaryKey: // only keep primary key with single int
				if len(c.Keys) != 1 {
					continue
				}
				key := c.Keys[0]
				if intColSet.Exist(key.Column.Name.L) {
					cs = append(cs, c)
				}
			default:
			}
		}
		s.Constraints = cs
	}
	return s
}

func filterOutAllConstraints(s ast.StmtNode) ast.StmtNode {
	if s, ok := s.(*ast.CreateTableStmt); ok {
		s.Constraints = []*ast.Constraint{}
		// todo: check whether required to filter out column options like `primary key`, since we may be able to handle it ?
	}
	return s
}
