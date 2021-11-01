package mask

import (
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/stretchr/testify/require"
)

func TestItWorks(t *testing.T) {
	t.Parallel()

	tables := map[string]string{
		"test.t": "test.TABLE0",
	}
	columns := map[string]string{
		"test.t.id":    "test.TABLE0.COL0",
		"test.t.name":  "test.TABLE0.COL1",
		"test.t.birth": "test.TABLE0.COL2",
	}

	global := NewGlobalNameMap(tables, columns)

	localColumns := []*expression.Column{
		{OrigName: "test.t.id"},
		{OrigName: "test.t.name"},
	}
	local, _ := NewLocalNameMap(global, localColumns)

	require.Equal(t, local.Column("unknown"), "unknown")
	require.Equal(t, local.Column("id"), "COL0")
	require.Equal(t, local.Column("t1.id"), "t1.COL0")
	require.Equal(t, local.Column("t.id"), "TABLE0.COL0")
	require.Equal(t, local.Column("t.unknown"), "t.unknown")
	require.Equal(t, local.Column("test.t.id"), "test.TABLE0.COL0")
}
