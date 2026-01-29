package merkletree

import (
    "fmt"
    "github.com/zeebo/blake3"
)

// MerkleProof представляет доказательство включения дерева в глобальный корень
type MerkleProof struct {
    TreeName   string      // Имя дерева
    TreeRoot   [32]byte    // Корень дерева
    ProofPath  [][32]byte  // Путь хешей для верификации
    IsLeft     []bool      // Флаги позиции (true = текущий узел слева)
    GlobalRoot [32]byte    // Глобальный корень для верификации
}

// Verify проверяет корректность Merkle proof
func (proof *MerkleProof) Verify() bool {
    currentHash := proof.TreeRoot
    
    // Идем снизу вверх по дереву
    for i := 0; i < len(proof.ProofPath); i++ {
        hasher := blake3.New()
        
        if proof.IsLeft[i] {
            // Текущий хеш слева
            hasher.Write(currentHash[:])
            hasher.Write(proof.ProofPath[i][:])
        } else {
            // Текущий хеш справа
            hasher.Write(proof.ProofPath[i][:])
            hasher.Write(currentHash[:])
        }
        
        copy(currentHash[:], hasher.Sum(nil))
    }
    
    return currentHash == proof.GlobalRoot
}

// String возвращает строковое представление proof
func (proof *MerkleProof) String() string {
    s := fmt.Sprintf("MerkleProof для '%s'\n", proof.TreeName)
    s += fmt.Sprintf("  Корень дерева: %x\n", proof.TreeRoot[:16])
    s += fmt.Sprintf("  Глобальный корень: %x\n", proof.GlobalRoot[:16])
    s += fmt.Sprintf("  Длина пути: %d\n", len(proof.ProofPath))
    s += "  Путь:\n"
    
    for i, hash := range proof.ProofPath {
        side := "right"
        if proof.IsLeft[i] {
            side = "left"
        }
        s += fmt.Sprintf("    [%d] %s: %x\n", i, side, hash[:16])
    }
    
    return s
}
