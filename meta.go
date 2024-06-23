package sstable

import (
	"encoding/binary"
)

var (
	byteOrdering = binary.LittleEndian
)

type TableMeta struct {
	DiskSize  uint32
	KeyCount  uint32
	Offsets   []uint32
	TableName []byte
}

func (t *TableMeta) Size() int {
	return 8 + int(4*t.KeyCount) + len(t.TableName)
}

func NewTableMeta(tableName string, size uint32) *TableMeta {
	t := &TableMeta{
		KeyCount:  size,
		Offsets:   make([]uint32, size),
		TableName: []byte(tableName),
	}
	t.DiskSize = uint32(t.Size())
	return t
}

func (t *TableMeta) ToBytes() ([]byte, error) {
	contents := make([]byte, t.Size())
	offset := 0
	byteOrdering.PutUint32(contents[offset:], t.DiskSize)
	offset += 4
	byteOrdering.PutUint32(contents[offset:], t.KeyCount)
	offset += 4
	for _, off := range t.Offsets {
		byteOrdering.PutUint32(contents[offset:], off)
		offset += 4
	}
	copy(contents[offset:], t.TableName)
	return contents, nil
}

func TableMetaFromBytes(contents []byte) *TableMeta {
	t := &TableMeta{}

	t.DiskSize = byteOrdering.Uint32(contents)
	t.KeyCount = byteOrdering.Uint32(contents[4:])

	t.Offsets = make([]uint32, t.KeyCount)
	offset := 8
	for i := 0; i < int(t.KeyCount); i++ {
		t.Offsets[i] = byteOrdering.Uint32(contents[offset:])
		offset += 4
	}
	t.TableName = contents[offset:]
	return t
}
