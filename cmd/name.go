package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"go.uber.org/zap"
)

type NameOption struct {
	MaskedDBPrefix string `opts:"help=prefix of masked schema files"`
	Output         string `opts:"help=path to the output name map"`
}

func (opt *NameOption) isMaskedInfo(info *ddlInfo) bool {
	return strings.HasPrefix(info.db, opt.MaskedDBPrefix)
}

func (opt *NameOption) handleDir(dir string, columns map[string]string) error {
	ddlPaths, _ := filepath.Glob(filepath.Join(dir, "*.*-schema.sql"))

	origInfos := []*ddlInfo{}
	maskedInfos := []*ddlInfo{}
	for _, path := range ddlPaths {
		info, err := newDDLInfo(path)
		if err != nil {
			return err
		}
		if opt.isMaskedInfo(info) {
			maskedInfos = append(maskedInfos, info)
		} else {
			origInfos = append(origInfos, info)
		}
	}

	if len(origInfos) != len(maskedInfos) {
		return fmt.Errorf("bad number of masked ddls")
	}
	sort.Sort(bySchemaName(origInfos))
	sort.Sort(bySchemaName(maskedInfos))

	for i := range origInfos {
		o := origInfos[i]
		m := maskedInfos[i]

		if len(o.stmt.Cols) != len(m.stmt.Cols) {
			return fmt.Errorf("bad number of columns for `%s` and `%s`", o.Prefix(), m.Prefix())
		}

		for j := range o.stmt.Cols {
			oCol := fmt.Sprintf("%s.%s", o.Prefix(), o.stmt.Cols[j].Name.Name.L)
			mCol := fmt.Sprintf("%s.%s", m.Prefix(), m.stmt.Cols[j].Name.Name.L)
			columns[oCol] = mCol
		}
	}

	return nil
}

func (opt *NameOption) Run() error {
	opt.MaskedDBPrefix = strings.ToLower(opt.MaskedDBPrefix)

	columns := map[string]string{}

	for _, dir := range globalOption.DDLDir {
		opt.handleDir(dir, columns)
	}

	nameMap := mask.NewGlobalNameMap(columns)
	bytes, err := json.MarshalIndent(nameMap, "", "\t")
	if err != nil {
		return err
	}
	err = os.WriteFile(opt.Output, bytes, 0666)
	if err != nil {
		return err
	}

	if len := len(nameMap.Columns); len == 0 {
		zap.S().Warnw("empty name map, no columns found")
	} else {
		zap.S().Infow("generated name map", "columns", len, "path", opt.Output)
	}

	return nil
}

func newDDLInfo(path string) (*ddlInfo, error) {
	base := filepath.Base(path)
	prefix := strings.ToLower(strings.TrimSuffix(base, "-schema.sql"))

	tokens := strings.Split(prefix, ".")
	if len(tokens) != 2 {
		return nil, fmt.Errorf("bad schema file name: `%s`", base)
	}
	db, table := tokens[0], tokens[1]

	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	str := string(bytes)
	p := parser.New()
	node, err := p.ParseOneStmt(str, "", "")
	if err != nil {
		return nil, err
	}

	if create, ok := node.(*ast.CreateTableStmt); ok {
		return &ddlInfo{
			db, table, create,
		}, nil
	}
	return nil, fmt.Errorf("not a create table statement in `%s`", path)
}

type ddlInfo struct {
	db    string
	table string
	stmt  *ast.CreateTableStmt
}

func (info *ddlInfo) Prefix() string {
	return fmt.Sprintf("%s.%s", info.db, info.table)
}

type bySchemaName []*ddlInfo

func (infos bySchemaName) Len() int { return len(infos) }
func (infos bySchemaName) Less(i, j int) bool {
	if infos[i].db == infos[j].db {
		return infos[i].table < infos[j].table
	} else {
		return infos[i].db < infos[j].db
	}
}
func (infos bySchemaName) Swap(i, j int) { infos[i], infos[j] = infos[j], infos[i] }
