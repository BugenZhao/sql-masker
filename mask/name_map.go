package mask

import (
	"fmt"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
)

func NewGlobalNameMap(tables map[string]string, columns map[string]string) *NameMap {
	return &NameMap{
		tables:  tables,
		columns: columns,
	}
}

func NewLocalNameMap(global *NameMap, columnsSubSet []*expression.Column) (*NameMap, error) {
	if global == nil {
		return nil, nil
	}

	columns := make(map[string]string)

	for _, col := range columnsSubSet {
		origName := col.OrigName
		mappedName, err := global.MustColumn(origName)
		if err != nil {
			return nil, err
		}

		origTokens := strings.Split(origName, ".")
		mappedTokens := strings.Split(mappedName, ".")
		for i := 0; i < len(origTokens); i++ {
			origSuffix := strings.Join(origTokens[i:], ".")
			mappedSuffix := strings.Join(mappedTokens[i:], ".")
			columns[origSuffix] = mappedSuffix
		}
	}

	return &NameMap{
		tables:  global.tables,
		columns: columns,
	}, nil
}

type NameMap struct {
	tables  map[string]string
	columns map[string]string
}

func nameMapFind(from string, m map[string]string) (string, error) {
	tokens := strings.Split(from, ".")
	for i := 0; i < len(tokens); i++ {
		suffix := strings.Join(tokens[i:], ".")
		if mappedSuffix, ok := m[suffix]; ok {
			mapped := tokens[:i]
			mapped = append(mapped, mappedSuffix)
			return strings.Join(mapped, "."), nil
		}
	}
	return from, fmt.Errorf("entry `%v` not found in name map", from)
}

func (m *NameMap) Column(from string) string {
	to, _ := nameMapFind(from, m.columns)
	return to
}

func (m *NameMap) MustColumn(from string) (string, error) {
	return nameMapFind(from, m.columns)
}

func (m *NameMap) ColumnName(name *ast.ColumnName) *ast.ColumnName {
	mapped := m.Column(name.String())
	tokens := strings.Split(mapped, ".")
	if len(tokens) >= 1 {
		name.Name = model.NewCIStr(tokens[len(tokens)-1])
	}
	if len(tokens) >= 2 {
		name.Table = model.NewCIStr(tokens[len(tokens)-2])
	}
	if len(tokens) == 3 {
		name.Table = model.NewCIStr(tokens[len(tokens)-3])
	}
	return name
}

func (m *NameMap) Table(from string) string {
	to, _ := nameMapFind(from, m.tables)
	return to
}

func (m *NameMap) TableName(name *ast.TableName) *ast.TableName {
	var from string
	if name.Schema.String() == "" {
		from = name.Name.String()
	} else {
		from = fmt.Sprintf("%v.%v", name.Schema, name.Name)
	}
	mapped := m.Table(from)
	tokens := strings.Split(mapped, ".")
	if len(tokens) >= 1 {
		name.Name = model.NewCIStr(tokens[len(tokens)-1])
	}
	if len(tokens) >= 2 {
		name.Name = model.NewCIStr(tokens[len(tokens)-2])
	}
	return name
}
