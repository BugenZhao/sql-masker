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
)

type NameOption struct {
	MaskedDBPrefix string
}

func (opt *NameOption) handleDir(dir string, tables map[string]string, columns map[string]string) error {
	ddls, _ := filepath.Glob(filepath.Join(dir, "*.*-schema.sql"))

	origDDLs := []string{}
	maskedDDLs := []string{}
	for _, ddl := range ddls {
		if strings.HasPrefix(strings.ToLower(filepath.Base(ddl)), opt.MaskedDBPrefix) {
			maskedDDLs = append(maskedDDLs, ddl)
		} else {
			origDDLs = append(origDDLs, ddl)
		}
	}

	if len(origDDLs) != len(maskedDDLs) {
		return fmt.Errorf("bad number of masked ddls")
	}
	sort.Strings(origDDLs)
	sort.Strings(maskedDDLs)

	p := parser.New()
	extract := func(path string) (string, *ast.CreateTableStmt, error) {
		base := filepath.Base(path)
		prefix := strings.ToLower(strings.TrimSuffix(base, "-schema.sql"))

		bytes, err := os.ReadFile(path)
		if err != nil {
			return prefix, nil, err
		}
		str := string(bytes)
		node, err := p.ParseOneStmt(str, "", "")
		if err != nil {
			return prefix, nil, err
		}
		if create, ok := node.(*ast.CreateTableStmt); ok {
			return prefix, create, nil
		}
		return prefix, nil, fmt.Errorf("not a create table statement in `%s`", path)
	}

	for i := range origDDLs {
		oPrefix, o, err := extract(origDDLs[i])
		if err != nil {
			return err
		}
		mPrefix, m, err := extract(maskedDDLs[i])
		if err != nil {
			return err
		}

		if len(o.Cols) != len(m.Cols) {
			return fmt.Errorf("bad number of columns for `%s` and `%s`", oPrefix, mPrefix)
		}

		tables[oPrefix] = mPrefix
		for j := range o.Cols {
			oCol := fmt.Sprintf("%s.%s", oPrefix, o.Cols[j].Name.Name.L)
			mCol := fmt.Sprintf("%s.%s", mPrefix, m.Cols[j].Name.Name.L)
			columns[oCol] = mCol
		}
	}

	return nil
}

func (opt *NameOption) Run() error {
	opt.MaskedDBPrefix = strings.ToLower(opt.MaskedDBPrefix)

	tables := map[string]string{}
	columns := map[string]string{}

	for _, dir := range globalOption.DDLDir {
		opt.handleDir(dir, tables, columns)
	}

	nameMap := mask.NewGlobalNameMap(tables, columns)
	bytes, err := json.MarshalIndent(nameMap, "", "\t")
	if err != nil {
		return err
	}
	fmt.Printf("%s", bytes)

	return nil
}
