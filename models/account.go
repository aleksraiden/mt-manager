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
	_data		[]byte
}

// ID реализует интерфейс Hashable
func (a *Account) ID() uint64 {
	return a.UID
}

// Key реализует интерфейс Hashable
func (a *Account) Key() [8]byte {
	return a.key
}

func (a *Account) Clear() {
	a._data = a._data[:0]
}

// Hash реализует интерфейс Hashable
func (a *Account) Hash() [32]byte {
	hasher := blake3.New()
	
	buf, err := a.Serialize()
	if len(buf) > 0 && err == nil {	
		hasher.Write(buf[:])	
	}	

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
		_data: make([]byte, 0, 64),
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
	if len(a._data) > 0 {
		return a._data, nil	//Отдаем кеш
	}
	
	if cap(a._data) >= 57 {
        a._data = a._data[:57]
    } else {
        a._data = make([]byte, 0, 57)
    }
	
	copy(a._data[0:32], a.PublicKey[:])
	binary.BigEndian.PutUint64(a._data[32:40], a.UID)
	copy(a._data[40:48], a.key[:])
	binary.BigEndian.PutUint64(a._data[48:56], a.EmailHash)
	a._data[56] = byte(a.Status)
	
	return a._data, nil
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
	
	// сразу кешируем — следующий Serialize() не будет пересчитывать
    if cap(a._data) >= 57 {
        a._data = a._data[:57]
    } else {
        a._data = make([]byte, 0, 57)
    }
    copy(a._data, data[:57])
	
	return nil
}

