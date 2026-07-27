package storage

import (
	"testing"

	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_CRUD(t *testing.T) {
	table := NewTable("test", []Column{{Name: "val", Type: types.TypeInt}})

	table.PutRow("key1", []types.Value{types.IntVal(42)})
	v, ok := table.GetRow("key1")
	require.True(t, ok)
	assert.Equal(t, int64(42), v[0].Int)

	ok = table.DeleteRow("key1")
	assert.True(t, ok)

	_, ok = table.GetRow("key1")
	assert.False(t, ok)
}

func TestTable_Scan(t *testing.T) {
	table := NewTable("test", []Column{{Name: "val", Type: types.TypeInt}})
	for i := 1; i <= 5; i++ {
		table.PutRow(RowKey(string(rune('0'+i))), []types.Value{types.IntVal(int64(i))})
	}
	rows := table.Scan(func(r Row) bool { return r.Values[0].Int > 3 })
	assert.Len(t, rows, 2)
}

func TestSeed_BalanceInvariant(t *testing.T) {
	catalog := NewCatalog()
	SeedCatalog(catalog)

	accounts, ok := catalog.Lookup("accounts")
	require.True(t, ok)

	rows := accounts.Scan(nil)
	var sum float64
	for _, row := range rows {
		sum += row.Values[2].Float // balance column
	}
	assert.InDelta(t, 1_000_000.0, sum, 0.01, "accounts balance invariant")
}

func TestCatalog_RegisterAndLookup(t *testing.T) {
	catalog := NewCatalog()
	t1 := NewTable("t1", nil)
	t2 := NewTable("t2", nil)

	catalog.Register(t1)
	catalog.Register(t2)

	got, ok := catalog.Lookup("t1")
	require.True(t, ok)
	assert.Equal(t, "t1", got.TableName())

	got, ok = catalog.Lookup("t2")
	require.True(t, ok)
	assert.Equal(t, "t2", got.TableName())

	_, ok = catalog.Lookup("nonexistent")
	assert.False(t, ok)
}

func TestCatalog_List(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register(NewTable("z", nil))
	catalog.Register(NewTable("a", nil))
	catalog.Register(NewTable("m", nil))

	names := catalog.List()
	assert.Equal(t, []string{"a", "m", "z"}, names)
}

func TestTable_Count(t *testing.T) {
	table := NewTable("t", []Column{{Name: "v", Type: types.TypeInt}})
	assert.Equal(t, 0, table.Count())

	table.PutRow("k1", []types.Value{types.IntVal(1)})
	assert.Equal(t, 1, table.Count())

	table.PutRow("k2", []types.Value{types.IntVal(2)})
	assert.Equal(t, 2, table.Count())

	table.DeleteRow("k1")
	assert.Equal(t, 1, table.Count())
}

func TestTable_Keys(t *testing.T) {
	table := NewTable("t", []Column{{Name: "v", Type: types.TypeInt}})
	table.PutRow("b", []types.Value{types.IntVal(2)})
	table.PutRow("a", []types.Value{types.IntVal(1)})
	table.PutRow("c", []types.Value{types.IntVal(3)})

	keys := table.Keys()
	assert.Equal(t, []RowKey{"a", "b", "c"}, keys)
}

func TestTable_Scan_NilFilter(t *testing.T) {
	table := NewTable("t", []Column{{Name: "v", Type: types.TypeInt}})
	table.PutRow("k1", []types.Value{types.IntVal(1)})
	table.PutRow("k2", []types.Value{types.IntVal(2)})

	rows := table.Scan(nil)
	assert.Len(t, rows, 2)
}

func TestTable_DeleteRow_Missing(t *testing.T) {
	table := NewTable("t", nil)
	assert.False(t, table.DeleteRow("nonexistent"))
}

func TestSeed_AllTablesRegistered(t *testing.T) {
	catalog := NewCatalog()
	SeedCatalog(catalog)

	_, ok := catalog.Lookup("accounts")
	assert.True(t, ok)
	_, ok = catalog.Lookup("products")
	assert.True(t, ok)
	_, ok = catalog.Lookup("inventory")
	assert.True(t, ok)
	assert.Len(t, catalog.List(), 3)
}

func TestSeed_ProductsScan(t *testing.T) {
	catalog := NewCatalog()
	SeedCatalog(catalog)

	products, ok := catalog.Lookup("products")
	require.True(t, ok)
	assert.Equal(t, 50, products.Count())
}

func TestSeed_InventoryCount(t *testing.T) {
	catalog := NewCatalog()
	SeedCatalog(catalog)

	inventory, ok := catalog.Lookup("inventory")
	require.True(t, ok)
	assert.Equal(t, 50, inventory.Count())
}
