package models

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/zeebo/blake3"
)

// AccountStatus статус аккаунта
type AccountStatus uint8

const (
	StatusSystem AccountStatus = iota
	StatusBlocked
	StatusMM
	StatusAlgo
	StatusUser
)

func (s AccountStatus) String() string {
	names := [...]string{"system", "blocked", "mm", "algo", "user"}
	if int(s) < len(names) {
		return names[s]
	}
	return "unknown"
}

// Account представляет аккаунт пользователя
type Account struct {
	PublicKey [32]byte
	UID       uint64
	key       [8]byte // Кешированный ключ
	EmailHash uint64
	Status    AccountStatus
}

// ID реализует интерфейс Hashable
func (a *Account) ID() uint64 {
	return a.UID
}

// Key реализует интерфейс Hashable
func (a *Account) Key() [8]byte {
	return a.key
}

// Hash реализует интерфейс Hashable
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

// NewAccount создает новый аккаунт
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

// NewAccountFactory фабрика для создания пустых аккаунтов
func NewAccountFactory() *Account {
	return &Account{}
}

// Serialize реализует интерфейс Serializable для Account
// Layout: [PublicKey 32][UID 8][key 8][EmailHash 8][Status 1] = 57 bytes
func (a *Account) Serialize() ([]byte, error) {
	buf := make([]byte, 57)
	copy(buf[0:32], a.PublicKey[:])
	binary.BigEndian.PutUint64(buf[32:40], a.UID)
	copy(buf[40:48], a.key[:])
	binary.BigEndian.PutUint64(buf[48:56], a.EmailHash)
	buf[56] = byte(a.Status)
	return buf, nil
}

// Deserialize реализует интерфейс Serializable для Account
func (a *Account) Deserialize(data []byte) error {
	if len(data) < 57 {
		return fmt.Errorf("account: need 57 bytes, got %d", len(data))
	}
	copy(a.PublicKey[:], data[0:32])
	a.UID = binary.BigEndian.Uint64(data[32:40])
	copy(a.key[:], data[40:48])
	a.EmailHash = binary.BigEndian.Uint64(data[48:56])
	a.Status = AccountStatus(data[56])
	return nil
}
