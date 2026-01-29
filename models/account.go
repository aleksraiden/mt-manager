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

// Serialize реализует интерфейс Serializable
func (a *Account) Serialize() []byte {
	buf := make([]byte, 32+8+8+8+1) // PublicKey + UID + key + EmailHash + Status
	offset := 0
	
	// PublicKey (32 bytes)
	copy(buf[offset:], a.PublicKey[:])
	offset += 32
	
	// UID (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], a.UID)
	offset += 8
	
	// key (8 bytes)
	copy(buf[offset:], a.key[:])
	offset += 8
	
	// EmailHash (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], a.EmailHash)
	offset += 8
	
	// Status (1 byte)
	buf[offset] = byte(a.Status)
	
	return buf
}

// Deserialize реализует интерфейс Serializable
func (a *Account) Deserialize(data []byte) error {
	if len(data) < 57 {
		return fmt.Errorf("invalid account data: expected at least 57 bytes, got %d", len(data))
	}
	
	offset := 0
	
	// PublicKey
	copy(a.PublicKey[:], data[offset:offset+32])
	offset += 32
	
	// UID
	a.UID = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	
	// key
	copy(a.key[:], data[offset:offset+8])
	offset += 8
	
	// EmailHash
	a.EmailHash = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	
	// Status
	a.Status = AccountStatus(data[offset])
	
	return nil
}
