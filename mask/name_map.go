package mask

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/zeebo/blake3"
)

const (
	defaultContext = "tidb"
)

func NewDictionary() *dictionary {
	return &dictionary{
		hasher: blake3.NewDeriveKey(defaultContext),
		dict:   make(map[string]uint32),
		values: make(map[uint32]bool),
	}
}

type dictionary struct {
	hasher *blake3.Hasher

	dict   map[string]uint32
	values map[uint32]bool
}

func (d *dictionary) get(key string) string {
	if value, ok := d.dict[key]; ok {
		return fmt.Sprintf("_h%s", strconv.FormatUint(uint64(value), 36))
	}
	return ""
}

func (d *dictionary) Map(key string) string {
	value := d.get(key)
	if value != "" {
		return value
	}

	d.hasher.Reset()
	_, _ = d.hasher.Write([]byte(key))
	sum := make([]byte, 4)
	_, err := d.hasher.Digest().Read(sum)
	if err != nil {
		panic(err)
	}
	u := binary.LittleEndian.Uint32(sum)

	for {
		if d.values[u] {
			u += 1
			continue
		}
		d.values[u] = true
		d.dict[key] = u
		return d.get(key)
	}
}

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
		mappedName := global.column(origName)
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
		dict:      NewDictionary(),
	}, nil
}

type NameMap struct {
	DBs     map[string]string `json:"dbs"`
	Tables  map[string]string `json:"tables"`
	Columns map[string]string `json:"columns"`

	dict      *dictionary
	currentDB string
}

func nameMapFind(from string, m map[string]string) (prefix []string, mappedSuffix string, _ error) {
	if from == "" {
		return nil, "", nil
	}

	from = strings.ToLower(from)
	tokens := strings.Split(from, ".")
	for i := 0; i < len(tokens); i++ {
		suffix := strings.Join(tokens[i:], ".")
		if mappedSuffix, ok := m[suffix]; ok {
			return tokens[:i], mappedSuffix, nil
		}
	}
	return tokens, "", fmt.Errorf("entry `%v` not found in name map", from)
}

func joinMapped(prefix []string, mappedSuffix string) string {
	if mappedSuffix != "" {
		prefix = append(prefix, mappedSuffix)
	}
	return strings.Join(prefix, ".")
}

func (m *NameMap) mapPrefix(from []string) []string {
	if m.dict == nil {
		return from
	}

	to := []string{}
	for _, key := range from {
		to = append(to, m.dict.Map(key))
	}
	return to
}

func (m *NameMap) column(from string) string {
	prefix, mappedSuffix, _ := nameMapFind(from, m.Columns)
	prefix = m.mapPrefix(prefix)
	to := joinMapped(prefix, mappedSuffix)
	return to
}

func (m *NameMap) ColumnName(name *ast.ColumnName) *ast.ColumnName {
	mapped := m.column(name.String())
	tokens := strings.Split(mapped, ".")
	if len(tokens) >= 1 {
		name.Name = model.NewCIStr(tokens[len(tokens)-1])
	}
	if len(tokens) >= 2 {
		name.Table = model.NewCIStr(tokens[len(tokens)-2])
	}
	if len(tokens) == 3 {
		name.Schema = model.NewCIStr(tokens[len(tokens)-3])
	}
	return name
}

func (m *NameMap) table(from string) string {
	if m.currentDB != "" && !strings.Contains(from, ".") {
		from = fmt.Sprintf("%s.%s", m.currentDB, from)
		prefix, mappedSuffix, _ := nameMapFind(from, m.Tables)
		prefix = m.mapPrefix(prefix)
		to := joinMapped(prefix, mappedSuffix)
		return strings.Split(to, ".")[1]
	} else {
		prefix, mappedSuffix, _ := nameMapFind(from, m.Tables)
		prefix = m.mapPrefix(prefix)
		to := joinMapped(prefix, mappedSuffix)
		return to
	}
}

func (m *NameMap) TableName(name *ast.TableName) *ast.TableName {
	var from string
	if name.Schema.L == "" {
		from = name.Name.String()
	} else {
		from = fmt.Sprintf("%v.%v", name.Schema, name.Name)
	}
	mapped := m.table(from)
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
