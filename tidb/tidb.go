package tidb

import (
	"context"
	"fmt"

	"github.com/pingcap/parser/ast"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
)

const (
	DefaultSocket   = "/tmp/sql-masker-tidb-socket"
	DefaultConnOpts = "charset=utf8mb4"
)

type TiDBInstance struct {
	ctx  context.Context
	qctx *server.TiDBContext
}

func NewTiDBInstance() (*TiDBInstance, error) {
	storage, err := mockstore.NewMockStore()
	if err != nil {
		return nil, err
	}
	session.DisableStats4Test()
	session.BootstrapSession(storage)
	driver := server.NewTiDBDriver(storage)

	qctx, err := driver.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "", nil)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	db := &TiDBInstance{
		ctx,
		qctx,
	}
	return db, nil
}

func (db *TiDBInstance) ParseOne(sql string) (ast.StmtNode, error) {
	stmts, err := db.qctx.Parse(db.ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("multiple stmt found")
	}
	return stmts[0], nil
}

func (db *TiDBInstance) Execute(sql string) (server.ResultSet, error) {
	stmt, err := db.ParseOne(sql)
	if err != nil {
		return nil, err
	}
	return db.qctx.ExecuteStmt(db.ctx, stmt)
}

func (db *TiDBInstance) Compile(sql string) (*executor.ExecStmt, error) {
	stmt, err := db.ParseOne(sql)
	if err != nil {
		return nil, err
	}
	compiler := executor.Compiler{Ctx: db.qctx.Session}
	execStmt, err := compiler.Compile(db.ctx, stmt)
	if err != nil {
		return nil, err
	}
	return execStmt, nil
}
