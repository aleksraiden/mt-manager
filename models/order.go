package models

import (
	"encoding/binary"
	"fmt"
	"github.com/zeebo/blake3"
)

// OrderSide сторона ордера
type OrderSide uint8

const (
	Buy OrderSide = iota
	Sell
)

// OrderType тип ордера
type OrderType uint8

const (
	Limit OrderType = iota
	Market
	StopLimit
	StopMarket
)

// Order представляет торговый ордер
type Order struct {
	OrderID   uint64    // Уникальный ID ордера
	UserID    uint64    // ID пользователя
	Symbol    uint32    // ID торговой пары (BTC/USD = 1, ETH/USD = 2, ...)
	Price     uint64    // Цена (в микро-единицах, price * 1e6)
	Quantity  uint64    // Количество
	Filled    uint64    // Заполнено
	Side      OrderSide // Buy/Sell
	Type      OrderType // Limit/Market/...
	Timestamp int64     // Unix timestamp
	key       [8]byte   // Кешированный ключ
}

// ID реализует интерфейс Hashable
func (o *Order) ID() uint64 {
	return o.OrderID
}

// Key реализует интерфейс Hashable
func (o *Order) Key() [8]byte {
	return o.key
}

// Hash реализует интерфейс Hashable
func (o *Order) Hash() [32]byte {
	hasher := blake3.New()
	hasher.Write(o.key[:])
	binary.Write(hasher, binary.BigEndian, o.UserID)
	binary.Write(hasher, binary.BigEndian, o.Symbol)
	binary.Write(hasher, binary.BigEndian, o.Price)
	binary.Write(hasher, binary.BigEndian, o.Quantity)
	binary.Write(hasher, binary.BigEndian, o.Filled)
	hasher.Write([]byte{byte(o.Side), byte(o.Type)})
	binary.Write(hasher, binary.BigEndian, o.Timestamp)
	var result [32]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

// NewOrder создает новый ордер
func NewOrder(orderID, userID uint64, symbol uint32, price, quantity uint64, side OrderSide, typ OrderType) *Order {
	order := &Order{
		OrderID:   orderID,
		UserID:    userID,
		Symbol:    symbol,
		Price:     price,
		Quantity:  quantity,
		Filled:    0,
		Side:      side,
		Type:      typ,
		Timestamp: 0, // можно time.Now().Unix()
	}
	binary.BigEndian.PutUint64(order.key[:], orderID)
	return order
}

// IsFilled проверяет, полностью ли заполнен ордер
func (o *Order) IsFilled() bool {
	return o.Filled >= o.Quantity
}

// RemainingQuantity возвращает оставшееся количество
func (o *Order) RemainingQuantity() uint64 {
	if o.Filled >= o.Quantity {
		return 0
	}
	return o.Quantity - o.Filled
}

// Serialize реализует интерфейс Serializable
func (o *Order) Serialize() []byte {
	buf := make([]byte, 8+8+4+8+8+8+1+1+8+8) // все поля
	offset := 0
	
	binary.BigEndian.PutUint64(buf[offset:], o.OrderID)
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], o.UserID)
	offset += 8
	binary.BigEndian.PutUint32(buf[offset:], o.Symbol)
	offset += 4
	binary.BigEndian.PutUint64(buf[offset:], o.Price)
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], o.Quantity)
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], o.Filled)
	offset += 8
	buf[offset] = byte(o.Side)
	offset++
	buf[offset] = byte(o.Type)
	offset++
	binary.BigEndian.PutUint64(buf[offset:], uint64(o.Timestamp))
	offset += 8
	copy(buf[offset:], o.key[:])
	
	return buf
}

// Deserialize реализует интерфейс Serializable
func (o *Order) Deserialize(data []byte) error {
	if len(data) < 62 {
		return fmt.Errorf("invalid order data: expected at least 62 bytes, got %d", len(data))
	}
	
	offset := 0
	o.OrderID = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	o.UserID = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	o.Symbol = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	o.Price = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	o.Quantity = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	o.Filled = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	o.Side = OrderSide(data[offset])
	offset++
	o.Type = OrderType(data[offset])
	offset++
	o.Timestamp = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8
	copy(o.key[:], data[offset:offset+8])
	
	return nil
}

// NewOrderFactory фабрика для создания пустых ордеров
func NewOrderFactory() *Order {
	return &Order{}
}