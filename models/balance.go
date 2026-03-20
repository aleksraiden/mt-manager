package models

import (
	"encoding/binary"
	"fmt"
	"github.com/zeebo/blake3"
)

// Balance представляет баланс пользователя по конкретному активу
type Balance struct {
	UserID    uint64 // ID пользователя
	AssetID   uint32 // ID актива (BTC=1, ETH=2, USD=3, ...)
	Available uint64 // Доступный баланс (в микро-единицах)
	Locked    uint64 // Заблокированный баланс (в ордерах)
	key       [8]byte
}

// ID реализует интерфейс Hashable
// Комбинируем UserID и AssetID для уникального ID
func (b *Balance) ID() uint64 {
	return (b.UserID << 32) | uint64(b.AssetID)
}

// Key реализует интерфейс Hashable
func (b *Balance) Key() [8]byte {
	return b.key
}

// Hash реализует интерфейс Hashable
func (b *Balance) Hash() [32]byte {
	hasher := blake3.New()
	hasher.Write(b.key[:])
	binary.Write(hasher, binary.BigEndian, b.UserID)
	binary.Write(hasher, binary.BigEndian, b.AssetID)
	binary.Write(hasher, binary.BigEndian, b.Available)
	binary.Write(hasher, binary.BigEndian, b.Locked)
	var result [32]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

// NewBalance создает новый баланс
func NewBalance(userID uint64, assetID uint32, available, locked uint64) *Balance {
	balance := &Balance{
		UserID:    userID,
		AssetID:   assetID,
		Available: available,
		Locked:    locked,
	}
	// Ключ = комбинация UserID и AssetID
	id := (userID << 32) | uint64(assetID)
	binary.BigEndian.PutUint64(balance.key[:], id)
	return balance
}

// TotalBalance возвращает общий баланс
func (b *Balance) TotalBalance() uint64 {
	return b.Available + b.Locked
}

// CanWithdraw проверяет, можно ли вывести указанную сумму
func (b *Balance) CanWithdraw(amount uint64) bool {
	return b.Available >= amount
}

// Balance layout: [key 8][UserID 8][AssetID 4][Available 8][Locked 8] = 36 bytes
func (b *Balance) Serialize() ([]byte, error) {
	buf := make([]byte, 36)
	copy(buf[0:8], b.key[:])
	binary.BigEndian.PutUint64(buf[8:16], b.UserID)
	binary.BigEndian.PutUint32(buf[16:20], b.AssetID)
	binary.BigEndian.PutUint64(buf[20:28], b.Available)
	binary.BigEndian.PutUint64(buf[28:36], b.Locked)
	return buf, nil
}

func (b *Balance) Deserialize(data []byte) error {
	if len(data) < 36 {
		return fmt.Errorf("balance: need 36 bytes, got %d", len(data))
	}
	copy(b.key[:], data[0:8])
	b.UserID = binary.BigEndian.Uint64(data[8:16])
	b.AssetID = binary.BigEndian.Uint32(data[16:20])
	b.Available = binary.BigEndian.Uint64(data[20:28])
	b.Locked = binary.BigEndian.Uint64(data[28:36])
	return nil
}

// NewBalanceFactory фабрика для создания пустых балансов
func NewBalanceFactory() *Balance {
	return &Balance{}
}