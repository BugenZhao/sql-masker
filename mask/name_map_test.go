package mask

import (
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/stretchr/testify/require"
)

func TestItWorks(t *testing.T) {
	t.Parallel()

	columns := map[string]string{
		"test.t.id":    "db0.table0.col0",
		"test.t.name":  "db0.table0.col1",
		"test.t.birth": "db0.table0.col2",
	}

	global := NewGlobalNameMap(columns)

	localColumns := []*expression.Column{
		{OrigName: "test.t.id"},
		{OrigName: "test.t.name"},
	}
	local, _ := NewLocalNameMap(global, localColumns, "test")

	require.Equal(t, local.Column("unknown"), "unknown")
	require.Equal(t, local.Column("id"), "col0")
	require.Equal(t, local.Column("t1.id"), "t1.col0")
	require.Equal(t, local.Column("t.id"), "table0.col0")
	require.Equal(t, local.Column("t.unknown"), "t.unknown")
	require.Equal(t, local.Column("unknown.id"), "unknown.col0")
	require.Equal(t, local.Column("test.t.id"), "db0.table0.col0")

	require.Equal(t, local.Table("test.t"), "db0.table0")
	require.Equal(t, local.Table("t"), "db0.table0")
}
