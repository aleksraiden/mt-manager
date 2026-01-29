package merkletree

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/zeebo/blake3"
)

// AccountStatus статус аккаунта для тестов
type AccountStatus uint8

const (
	StatusSystem AccountStatus = iota
	StatusBlocked
	StatusMM
	StatusAlgo
	StatusUser
)

// Account для тестов
type Account struct {
	PublicKey [32]byte
	UID       uint64
	key       [8]byte
	EmailHash uint64
	Status    AccountStatus
}

func (a *Account) ID() uint64 {
	return a.UID
}

func (a *Account) Key() [8]byte {
	return a.key
}

func (a *Account) Hash() [32]byte {
	hasher := blake3.New()
	hasher.Write(a.key[:])
	binary.Write(hasher, binary.BigEndian, a.EmailHash)
	hasher.Write([]byte{byte(a.Status)})
	hasher.Write(a.PublicKey[:])
	var result [32]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func NewAccount(uid uint64, status AccountStatus) *Account {
	acc := &Account{
		UID:       uid,
		Status:    status,
		EmailHash: uid ^ 0xCAFEBABE,
	}
	binary.BigEndian.PutUint64(acc.key[:], uid)
	rand.Read(acc.PublicKey[:])
	return acc
}

func NewAccountDeterministic(uid uint64, status AccountStatus) *Account {
	acc := &Account{
		UID:       uid,
		Status:    status,
		EmailHash: uid ^ 0xCAFEBABE,
	}
	binary.BigEndian.PutUint64(acc.key[:], uid)
	// Детерминированный PublicKey для тестов
	for i := range acc.PublicKey {
		acc.PublicKey[i] = byte((uid + uint64(i)) % 256)
	}
	return acc
}

// Balance для тестов
type Balance struct {
	UserID    uint64
	AssetID   uint32
	Available uint64
	Locked    uint64
	key       [8]byte
}

func (b *Balance) ID() uint64 {
	return (b.UserID << 32) | uint64(b.AssetID)
}

func (b *Balance) Key() [8]byte {
	return b.key
}

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

func NewBalance(userID uint64, assetID uint32, available, locked uint64) *Balance {
	balance := &Balance{
		UserID:    userID,
		AssetID:   assetID,
		Available: available,
		Locked:    locked,
	}
	id := (userID << 32) | uint64(assetID)
	binary.BigEndian.PutUint64(balance.key[:], id)
	return balance
}
