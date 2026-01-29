package main

import (
	"fmt"
	"mt-manager/merkletree"
	"mt-manager/models"
)

// rootHex возвращает первые 16 байт корня в hex формате
func rootHex(root [32]byte) string {
	return fmt.Sprintf("%x", root[:16])
}

func main() {
	// Создаем менеджер
	mgr := merkletree.NewManager[*models.Account](merkletree.MediumConfig())
	
	// Дерево для аккаунтов
	accountTree, _ := mgr.CreateTree("accounts")
	
	// Вставляем аккаунты
	accounts := []*models.Account{
		models.NewAccount(1, models.StatusUser),
		models.NewAccount(2, models.StatusUser),
		models.NewAccount(3, models.StatusMM),
	}
	accountTree.InsertBatch(accounts)
	
	// Теперь можно использовать напрямую
	fmt.Printf("Accounts tree root: %s\n", rootHex(accountTree.ComputeRoot()))
	
	// Создаем менеджер для ордеров
	orderMgr := merkletree.NewManager[*models.Order](merkletree.LargeConfig())
	orderTree, _ := orderMgr.CreateTree("orders")
	
	// Вставляем ордера
	orders := []*models.Order{
		models.NewOrder(1001, 1, 1, 50000_000000, 100_000000, models.Buy, models.Limit),
		models.NewOrder(1002, 2, 1, 51000_000000, 200_000000, models.Sell, models.Limit),
	}
	orderTree.InsertBatch(orders)
	
	fmt.Printf("Orders tree root: %s\n", rootHex(orderTree.ComputeRoot()))
	
	// Range-запрос ордеров
	rangeOrders := orderTree.RangeQueryByID(1000, 1010, true, false)
	fmt.Printf("Found %d orders in range [1000, 1010)\n", len(rangeOrders))
	
	// Создаем менеджер для балансов
	balanceMgr := merkletree.NewManager[*models.Balance](merkletree.MediumConfig())
	balanceTree, _ := balanceMgr.CreateTree("balances")
	
	// Вставляем балансы
	balances := []*models.Balance{
		models.NewBalance(1, 1, 1000_000000, 100_000000), // User 1, BTC
		models.NewBalance(1, 3, 10000_000000, 0),         // User 1, USD
		models.NewBalance(2, 1, 500_000000, 50_000000),   // User 2, BTC
	}
	balanceTree.InsertBatch(balances)
	
	fmt.Printf("Balances tree root: %s\n", rootHex(balanceTree.ComputeRoot()))
}

