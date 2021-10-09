package tidb

import (
	"context"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"go.uber.org/zap/zapcore"
)

const (
	DefaultSocket   = "/tmp/sql-masker-tidb-socket"
	DefaultConnOpts = "charset=utf8mb4"
)

type Instance struct {
	ctx  context.Context
	qctx *server.TiDBContext
}

func NewInstance() (*Instance, error) {
	log.SetLevel(zapcore.ErrorLevel)

	storage, err := mockstore.NewMockStore()
	if err != nil {
		return nil, err
	}
	session.DisableStats4Test()
	session.BootstrapSession(storage)
	driver := server.NewTiDBDriver(storage)

	qctx, err := driver.OpenCtx(uint64(0), 0, uint8(mysql.DefaultCollationID), "", nil)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	db := &Instance{
		ctx,
		qctx,
	}
	return db, nil
}

func (db *Instance) Parse(sql string) ([]ast.StmtNode, error) {
	stmts, err := db.qctx.Parse(db.ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("error parsing sql `%s`: %w", sql, err)
	}
	return stmts, nil
}

func (db *Instance) ParseOne(sql string) (ast.StmtNode, error) {
	stmts, err := db.Parse(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("not exactly one stmt")
	}
	return stmts[0], nil
}

func (db *Instance) ExecuteOne(sql string) (server.ResultSet, error) {
	stmt, err := db.ParseOne(sql)
	if err != nil {
		return nil, err
	}
	return db.qctx.ExecuteStmt(db.ctx, stmt)
}

func (db *Instance) Execute(sql string) error {
	stmts, err := db.Parse(sql)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		_, err := db.qctx.ExecuteStmt(db.ctx, stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Instance) CompileStmtNode(stmt ast.StmtNode) (*executor.ExecStmt, error) {
	compiler := executor.Compiler{Ctx: db.qctx.Session}
	execStmt, err := compiler.Compile(db.ctx, stmt)
	if err != nil {
		return nil, err
	}
	return execStmt, nil
}

func (db *Instance) Compile(sql string) (*executor.ExecStmt, error) {
	stmt, err := db.ParseOne(sql)
	if err != nil {
		return nil, err
	}
	return db.CompileStmtNode(stmt)
}
