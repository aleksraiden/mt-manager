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

// Serialize реализует интерфейс Serializable
func (b *Balance) Serialize() []byte {
	buf := make([]byte, 8+4+8+8+8) // UserID + AssetID + Available + Locked + key
	offset := 0
	
	binary.BigEndian.PutUint64(buf[offset:], b.UserID)
	offset += 8
	binary.BigEndian.PutUint32(buf[offset:], b.AssetID)
	offset += 4
	binary.BigEndian.PutUint64(buf[offset:], b.Available)
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], b.Locked)
	offset += 8
	copy(buf[offset:], b.key[:])
	
	return buf
}

// Deserialize реализует интерфейс Serializable
func (b *Balance) Deserialize(data []byte) error {
	if len(data) < 36 {
		return fmt.Errorf("invalid balance data: expected at least 36 bytes, got %d", len(data))
	}
	
	offset := 0
	b.UserID = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	b.AssetID = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	b.Available = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	b.Locked = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	copy(b.key[:], data[offset:offset+8])
	
	return nil
}

// NewBalanceFactory фабрика для создания пустых балансов
func NewBalanceFactory() *Balance {
	return &Balance{}
}