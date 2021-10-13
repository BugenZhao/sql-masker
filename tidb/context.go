package tidb

import (
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/server"
)

type Context struct {
	*Instance
	qctx *server.TiDBContext
}

func (db *Context) Parse(sql string) ([]ast.StmtNode, error) {
	stmts, err := db.qctx.Parse(db.ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("error parsing sql `%s`: %w", sql, err)
	}
	return stmts, nil
}

func (db *Context) ParseOne(sql string) (ast.StmtNode, error) {
	stmts, err := db.Parse(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("not exactly one stmt")
	}
	return stmts[0], nil
}

func (db *Context) ExecuteOne(sql string) (server.ResultSet, error) {
	stmt, err := db.ParseOne(sql)
	if err != nil {
		return nil, err
	}
	return db.qctx.ExecuteStmt(db.ctx, stmt)
}

type StmtTransform = func(ast.StmtNode) ast.StmtNode

func (db *Context) ExecuteWithTransform(sql string, transform StmtTransform) error {
	stmts, err := db.Parse(sql)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if transform != nil {
			stmt = transform(stmt)
		}
		_, err := db.qctx.ExecuteStmt(db.ctx, stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Context) Execute(sql string) error {
	return db.ExecuteWithTransform(sql, nil)
}

func (db *Context) CompileStmtNode(stmt ast.StmtNode) (*executor.ExecStmt, error) {
	compiler := executor.Compiler{Ctx: db.qctx.Session}
	execStmt, err := compiler.Compile(db.ctx, stmt)
	if err != nil {
		return nil, err
	}
	return execStmt, nil
}

func (db *Context) Compile(sql string) (*executor.ExecStmt, error) {
	stmt, err := db.ParseOne(sql)
	if err != nil {
		return nil, err
	}
	return db.CompileStmtNode(stmt)
}
