package mask

import (
	"fmt"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
)

func NewGlobalNameMap(columns map[string]string) *NameMap {
	dbs := map[string]string{}
	tables := map[string]string{}

	for from, to := range columns {
		fromTokens := strings.Split(from, ".")
		toTokens := strings.Split(to, ".")
		dbs[fromTokens[0]] = toTokens[0]
		tables[strings.Join(fromTokens[:2], ".")] = strings.Join(toTokens[:2], ".")
	}

	return &NameMap{
		DBs:     dbs,
		Tables:  tables,
		Columns: columns,
	}
}

func NewLocalNameMap(global *NameMap, columnsSubSet []*expression.Column, currentDB string) (*NameMap, error) {
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
		Tables:    global.Tables,
		Columns:   columns,
		currentDB: currentDB,
	}, nil
}

type NameMap struct {
	DBs     map[string]string `json:"dbs"`
	Tables  map[string]string `json:"tables"`
	Columns map[string]string `json:"columns"`

	currentDB string
}

func nameMapFind(from string, m map[string]string) (string, error) {
	if from == "" {
		return "", nil
	}

	from = strings.ToLower(from)
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
	to, _ := nameMapFind(from, m.Columns)
	return to
}

func (m *NameMap) MustColumn(from string) (string, error) {
	return nameMapFind(from, m.Columns)
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
	if m.currentDB != "" && !strings.Contains(from, ".") {
		from = fmt.Sprintf("%s.%s", m.currentDB, from)
	}

	to, _ := nameMapFind(from, m.Tables)
	return to
}

func (m *NameMap) TableName(name *ast.TableName) *ast.TableName {
	var from string
	if name.Schema.L == "" {
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
		name.Schema = model.NewCIStr(tokens[len(tokens)-2])
	}
	return name
}

func (m *NameMap) DB(from string) string {
	if to, ok := m.DBs[from]; ok {
		return to
	}
	return from
}
