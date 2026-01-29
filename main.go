package main

import (
	"fmt"
	"mt-manager/merkletree"
	"mt-manager/models"
	"time"
)

// rootHex возвращает первые 16 байт корня в hex формате
func rootHex(root [32]byte) string {
	return fmt.Sprintf("%x", root[:16])
}

func main() {
	// ✨ ГЛАВНОЕ ИЗМЕНЕНИЕ: Создаем ОДИН универсальный менеджер для всех типов данных!
	mgr := merkletree.NewUniversalManager(merkletree.MediumConfig())

	fmt.Println("=== Универсальный менеджер Merkle-деревьев ===")

	// Дерево для аккаунтов (тип *models.Account)
	accountTree, _ := merkletree.CreateTree[*models.Account](mgr, "accounts")
	accounts := []*models.Account{
		models.NewAccount(1, models.StatusUser),
		models.NewAccount(2, models.StatusUser),
		models.NewAccount(3, models.StatusMM),
	}
	accountTree.InsertBatch(accounts)
	fmt.Printf("Accounts tree root: %s\n", rootHex(accountTree.ComputeRoot()))

	// Дерево для ордеров (тип *models.Order) - В ТОМ ЖЕ менеджере!
	orderTree, _ := merkletree.CreateTree[*models.Order](mgr, "orders")
	orders := []*models.Order{
		models.NewOrder(1001, 1, 1, 50000_000000, 100_000000, models.Buy, models.Limit),
		models.NewOrder(1002, 2, 1, 51000_000000, 200_000000, models.Sell, models.Limit),
	}
	orderTree.InsertBatch(orders)
	fmt.Printf("Orders tree root: %s\n", rootHex(orderTree.ComputeRoot()))

	// Range-запрос ордеров
	rangeOrders := orderTree.RangeQueryByID(1000, 1010, true, false)
	fmt.Printf("Found %d orders in range [1000, 1010)\n", len(rangeOrders))

	// Дерево для балансов (тип *models.Balance) - В ТОМ ЖЕ менеджере!
	balanceTree, _ := merkletree.CreateTree[*models.Balance](mgr, "balances")
	balances := []*models.Balance{
		models.NewBalance(1, 1, 1000_000000, 100_000000),
		models.NewBalance(1, 3, 10000_000000, 0),
		models.NewBalance(2, 1, 500_000000, 50_000000),
	}
	balanceTree.InsertBatch(balances)
	fmt.Printf("Balances tree root: %s\n", rootHex(balanceTree.ComputeRoot()))

	// Дерево для транзакций (тип *models.Transaction) - В ТОМ ЖЕ менеджере!
	txTree, _ := merkletree.CreateTree[*models.Transaction](mgr, "transactions")
	transactions := []*models.Transaction{
		// NewTransaction(txID, userID, assetID, amount, typ, timestamp)
		models.NewTransaction(1, 1, 1, 100_000000, models.Trade, time.Now().Unix()),
		models.NewTransaction(2, 2, 1, 200_000000, models.Trade, time.Now().Unix()),
	}
	txTree.InsertBatch(transactions)
	fmt.Printf("Transactions tree root: %s\n", rootHex(txTree.ComputeRoot()))

	fmt.Println()

	// ✨ Теперь можем вычислить ОБЩИЙ корень для всех деревьев разных типов!
	globalRoot := mgr.ComputeGlobalRoot()
	fmt.Printf("Global root (all trees): %s\n", rootHex(globalRoot))

	fmt.Println()

	// Статистика всех деревьев
	stats := mgr.GetTotalStats()
	fmt.Printf("Total trees: %d\n", stats.TreeCount)
	fmt.Printf("Total items: %d\n", stats.TotalItems)
	fmt.Printf("Total nodes: %d\n", stats.TotalNodes)

	fmt.Println()

	// Список всех деревьев
	trees := mgr.ListTrees()
	fmt.Printf("All trees: %v\n", trees)

	fmt.Println()

	// Информация о каждом дереве
	for _, treeName := range trees {
		info, _ := mgr.GetTreeInfo(treeName)
		fmt.Printf("- %s: %d items, root=%s\n",
			info.Name, info.Size, rootHex(info.Root))
	}

	fmt.Println()

	// Проверка Merkle proof для дерева orders
	proof, _ := mgr.GetMerkleProof("orders")
	fmt.Printf("Merkle proof for 'orders' verified: %v\n", proof.Verify())

	fmt.Println()

	// Можем работать с деревьями типобезопасно
	// Получаем типизированное дерево
	if retrievedOrderTree, exists := merkletree.GetTree[*models.Order](mgr, "orders"); exists {
		fmt.Printf("Retrieved 'orders' tree: %d items\n", retrievedOrderTree.Size())

		// Можем вставлять новые элементы
		newOrder := models.NewOrder(1003, 3, 1, 52000_000000, 150_000000, models.Buy, models.Limit)
		retrievedOrderTree.Insert(newOrder)
		fmt.Printf("After insert: %d items\n", retrievedOrderTree.Size())
	}

	// Попытка получить дерево с неправильным типом вернет false
	if _, exists := merkletree.GetTree[*models.Account](mgr, "orders"); !exists {
		fmt.Println("✓ Type safety works: cannot get 'orders' as Account tree")
	}

	fmt.Println("\n=== Готово! ===")
}
