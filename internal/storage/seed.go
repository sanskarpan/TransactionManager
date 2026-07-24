package storage

import (
	"fmt"

	"github.com/sanskarpan/TransactionManager/internal/types"
)

// AccountColumns defines the schema for the bank accounts table.
var AccountColumns = []Column{
	{Name: "id", Type: types.TypeInt, NotNull: true, PK: true},
	{Name: "owner", Type: types.TypeText, NotNull: true},
	{Name: "balance", Type: types.TypeFloat, NotNull: true},
	{Name: "branch", Type: types.TypeText, NotNull: true},
}

// ProductColumns defines the schema for the products table.
var ProductColumns = []Column{
	{Name: "id", Type: types.TypeInt, NotNull: true, PK: true},
	{Name: "name", Type: types.TypeText, NotNull: true},
	{Name: "price", Type: types.TypeFloat, NotNull: true},
	{Name: "category", Type: types.TypeText, NotNull: true},
	{Name: "stock", Type: types.TypeInt, NotNull: true},
}

// InventoryColumns defines the schema for the inventory table.
var InventoryColumns = []Column{
	{Name: "product_id", Type: types.TypeInt, NotNull: true, PK: true},
	{Name: "warehouse", Type: types.TypeText, NotNull: true},
	{Name: "quantity", Type: types.TypeInt, NotNull: true},
}

// SeedCatalog populates the catalog with sample accounts, products, and inventory tables.
func SeedCatalog(catalog *Catalog) {
	accounts := NewTable("accounts", AccountColumns)
	branches := []string{"north", "south", "east", "west", "central"}
	balancePerAccount := 10000.0 // 100 accounts × 10000 = 1,000,000
	for i := 1; i <= 100; i++ {
		key := RowKey(fmt.Sprintf("%d", i))
		branch := branches[(i-1)%len(branches)]
		accounts.PutRow(key, []types.Value{
			types.IntVal(int64(i)),
			types.TextVal(fmt.Sprintf("owner-%d", i)),
			types.FloatVal(balancePerAccount),
			types.TextVal(branch),
		})
	}
	catalog.Register(accounts)

	products := NewTable("products", ProductColumns)
	categories := []string{"electronics", "clothing", "food", "books", "toys"}
	for i := 1; i <= 50; i++ {
		key := RowKey(fmt.Sprintf("%d", i))
		products.PutRow(key, []types.Value{
			types.IntVal(int64(i)),
			types.TextVal(fmt.Sprintf("product-%d", i)),
			types.FloatVal(float64(i) * 9.99),
			types.TextVal(categories[(i-1)%len(categories)]),
			types.IntVal(int64(i * 10)),
		})
	}
	catalog.Register(products)

	inventory := NewTable("inventory", InventoryColumns)
	warehouses := []string{"WH-A", "WH-B", "WH-C"}
	for i := 1; i <= 50; i++ {
		key := RowKey(fmt.Sprintf("%d", i))
		inventory.PutRow(key, []types.Value{
			types.IntVal(int64(i)),
			types.TextVal(warehouses[(i-1)%len(warehouses)]),
			types.IntVal(int64(i * 10)),
		})
	}
	catalog.Register(inventory)
}
