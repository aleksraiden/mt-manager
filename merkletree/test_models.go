package merkletree

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"

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
	StatusVIP
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
	hasher := blake3HasherPool.Get().(*blake3.Hasher)
	defer blake3HasherPool.Put(hasher)
	hasher.Reset()
	hasher.Write(a.key[:])
	binary.Write(hasher, binary.BigEndian, a.EmailHash)
	hasher.Write([]byte{byte(a.Status)})
	hasher.Write(a.PublicKey[:])
	var result [32]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

// Serialize реализует Serializable для *Account
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

// Deserialize реализует Serializable для *Account
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
	hasher := blake3HasherPool.Get().(*blake3.Hasher)
	defer blake3HasherPool.Put(hasher)
	hasher.Reset()
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

// Serialize реализует Serializable для *Balance
// Layout: [key 8][UserID 8][AssetID 4][Available 8][Locked 8] = 36 bytes
func (b *Balance) Serialize() ([]byte, error) {
	buf := make([]byte, 36)
	copy(buf[0:8], b.key[:])
	binary.BigEndian.PutUint64(buf[8:16], b.UserID)
	binary.BigEndian.PutUint32(buf[16:20], b.AssetID)
	binary.BigEndian.PutUint64(buf[20:28], b.Available)
	binary.BigEndian.PutUint64(buf[28:36], b.Locked)
	return buf, nil
}

// Deserialize реализует Serializable для *Balance
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
