package merkletree

import (
	"crypto/rand"
	"encoding/binary"
	"github.com/zeebo/blake3"
)

// AccountStatus для тестов
type AccountStatus uint8

const (
	StatusSystem AccountStatus = iota
	StatusBlocked
	StatusMM
	StatusAlgo
	StatusUser
)

// Account для тестов (упрощенная версия)
type Account struct {
	PublicKey [32]byte
	UID       uint64
	key       [8]byte
	EmailHash uint64
	Status    AccountStatus
	_         [7]byte
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

// NewAccountDeterministic создает аккаунт с детерминированным ключом (для тестов)
func NewAccountDeterministic(uid uint64, status AccountStatus) *Account {
	acc := &Account{
		UID:       uid,
		Status:    status,
		EmailHash: uid ^ 0xCAFEBABE,
	}

	binary.BigEndian.PutUint64(acc.key[:], uid)

	// Детерминированный публичный ключ на основе UID
	hasher := blake3.New()
	hasher.Write(acc.key[:])
	hasher.Write([]byte("deterministic_seed"))
	copy(acc.PublicKey[:], hasher.Sum(nil))

	return acc
}