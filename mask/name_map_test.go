package mask

import (
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/stretchr/testify/require"
)

func TestNameMap(t *testing.T) {
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

	require.Equal(t, local.column("unknown"), "_hldgmah")
	require.Equal(t, local.column("id"), "col0")
	require.Equal(t, local.column("t1.id"), "_h1y98qyh.col0")
	require.Equal(t, local.column("t.id"), "table0.col0")
	require.Equal(t, local.column("t.unknown"), "_hof3tpq._hldgmah")
	require.Equal(t, local.column("unknown.id"), "_hldgmah.col0")
	require.Equal(t, local.column("test.t.id"), "db0.table0.col0")

	require.Equal(t, local.table("test.t"), "db0.table0")
	require.Equal(t, local.table("t"), "table0")
}
