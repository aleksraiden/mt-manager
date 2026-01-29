package models

import (
	"encoding/binary"
	"github.com/zeebo/blake3"
)

// TxType тип транзакции
type TxType uint8

const (
	Deposit TxType = iota
	Withdrawal
	Trade
	Transfer
	Fee
)

// Transaction представляет транзакцию
type Transaction struct {
	TxID      uint64 // ID транзакции
	UserID    uint64 // ID пользователя
	AssetID   uint32 // ID актива
	Amount    uint64 // Сумма (в микро-единицах)
	Type      TxType // Тип транзакции
	Timestamp int64  // Unix timestamp
	key       [8]byte
}

// ID реализует интерфейс Hashable
func (t *Transaction) ID() uint64 {
	return t.TxID
}

// Key реализует интерфейс Hashable
func (t *Transaction) Key() [8]byte {
	return t.key
}

// Hash реализует интерфейс Hashable
func (t *Transaction) Hash() [32]byte {
	hasher := blake3.New()
	hasher.Write(t.key[:])
	binary.Write(hasher, binary.BigEndian, t.UserID)
	binary.Write(hasher, binary.BigEndian, t.AssetID)
	binary.Write(hasher, binary.BigEndian, t.Amount)
	hasher.Write([]byte{byte(t.Type)})
	binary.Write(hasher, binary.BigEndian, t.Timestamp)
	var result [32]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

// NewTransaction создает новую транзакцию
func NewTransaction(txID, userID uint64, assetID uint32, amount uint64, typ TxType, timestamp int64) *Transaction {
	tx := &Transaction{
		TxID:      txID,
		UserID:    userID,
		AssetID:   assetID,
		Amount:    amount,
		Type:      typ,
		Timestamp: timestamp,
	}
	binary.BigEndian.PutUint64(tx.key[:], txID)
	return tx
}
