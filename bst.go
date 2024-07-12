package sstable

import "bytes"

// TODO - Implment RedBlack tree which would have better worst case complexity
type Bst struct {
	Root *BstNode
}

func NewBst() *Bst {
	return &Bst{}
}

func (b *Bst) Insert(key, value []byte, atomicCount uint64) {
	node := &BstNode{
		Key:         key,
		Value:       value,
		AtomicCount: atomicCount,
	}
	if b.Root == nil {
		b.Root = node
		return
	}
	b.Root.Insert(node)
}

func (b *Bst) Contains(key []byte) (bool, error) {
	found := b.Root.SearchKey(key)
	return found != nil, nil
}

func (b *Bst) Get(key []byte) (*Record, error) {
	found := b.Root.SearchKey(key)
	return found, nil
}

func (b *Bst) Scan() ([]*Record, error) {
	var results []*Record
	inOrderTraverse(b.Root, &results)
	return results, nil
}

func (b *Bst) ScanWithLimit(limit *Limit) ([]*Record, error) {
	var results []*Record
	inOrderTraverseLimit(b.Root, &results, nil, int(limit.MaxResults))
	return results, nil
}

func (b *Bst) ScanWithPredicate(pred Predicate, limit *Limit) ([]*Record, error) {
	var results []*Record
	inOrderTraverseLimit(b.Root, &results, pred, int(limit.MaxResults))
	return results, nil
}

func (b *Bst) ToSSTable(tableName string) *SSTable {
	records, _ := b.Scan()
	return NewSSTable(tableName, records)
}

type BstNode struct {
	Key         []byte
	Value       []byte
	AtomicCount uint64
	Left        *BstNode
	Right       *BstNode
}

func (b *BstNode) Insert(node *BstNode) {

	cmp := bytes.Compare(b.Key, node.Key)
	if cmp == 0 {
		if node.AtomicCount > b.AtomicCount {
			b.Key = node.Key
			b.Value = node.Value
			b.AtomicCount = node.AtomicCount
			return
		}
	}
	if cmp < 0 {
		if b.Right == nil {
			b.Right = node
			return
		}
		b.Right.Insert(node)
	} else {
		if b.Left == nil {
			b.Left = node
			return
		}
		b.Left.Insert(node)
	}
}

func (b *BstNode) SearchKey(key []byte) *Record {
	if b == nil {
		return nil
	}

	cmp := bytes.Compare(b.Key, key)
	if cmp == 0 {
		return NewRecordWithCount(b.Key, b.Value, b.AtomicCount)
	}
	if cmp < 0 {
		return b.Right.SearchKey(key)
	} else {
		return b.Left.SearchKey(key)
	}
}

func inOrderTraverse(node *BstNode, results *[]*Record) {
	if node == nil {
		return
	}
	inOrderTraverse(node.Left, results)
	*results = append(*results, NewRecordWithCount(node.Key, node.Value, node.AtomicCount))
	inOrderTraverse(node.Right, results)
}

func inOrderTraverseLimit(node *BstNode, results *[]*Record, pred Predicate, limit int) {
	if node == nil || len(*results) == limit {
		return
	}
	inOrderTraverseLimit(node, results, pred, limit)
	if len(*results) == limit {
		return
	}
	if pred == nil || pred(node.Key, node.Value) {
		*results = append(*results, NewRecordWithCount(node.Key, node.Value, node.AtomicCount))
	}
	inOrderTraverseLimit(node, results, pred, limit)
}
