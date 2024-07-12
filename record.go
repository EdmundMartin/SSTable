package sstable

import (
	"bytes"
	"os"
)

// TODO - Update search to check for tombstone value
var TombstoneMarker = []byte("#DELETED#")

type Records []*Record

func (r Records) Len() int {
	return len(r)
}

func (r Records) Less(i, j int) bool {
	return bytes.Compare(r[i].Key, r[j].Key) < 0
}

func (r Records) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r Records) ToBytes() ([]byte, error) {
	allRecords := [][]byte{}
	for _, rec := range r {
		contents, err := rec.ToBytes()
		if err != nil {
			return nil, err
		}
		allRecords = append(allRecords, contents)
	}
	return bytes.Join(allRecords, nil), nil
}

func (r Records) Size() int {
	size := 0
	for _, rec := range r {
		size += rec.Size()
	}
	return size
}

type Record struct {
	KeySize     uint32
	Key         []byte
	ValueSize   uint32
	Value       []byte
	AtomicCount uint64
}

type AtomicCounter func() uint64

func NewRecordWithCount(key, value []byte, count uint64) *Record {
	return NewRecord(key, value, func() uint64 {
		return count
	})
}

func NewRecord(key, value []byte, countFunc AtomicCounter) *Record {
	r := &Record{
		KeySize:     uint32(len(key)),
		Key:         key,
		ValueSize:   uint32(len(value)),
		Value:       value,
		AtomicCount: countFunc(),
	}
	return r
}

func (r *Record) Deleted() bool {
	return bytes.Compare(r.Value, TombstoneMarker) == 0
}

func (r *Record) ToBytes() ([]byte, error) {
	contents := make([]byte, r.Size())

	byteOrdering.PutUint32(contents, r.KeySize)
	copy(contents[4:], r.Key)
	offset := 4 + len(r.Key)
	byteOrdering.PutUint32(contents[offset:], r.ValueSize)
	offset += 4
	copy(contents[offset:], r.Value)
	offset += len(r.Value)
	byteOrdering.PutUint64(contents[offset:], r.AtomicCount)
	return contents, nil
}

func RecordFromBytes(contents []byte) *Record {
	r := &Record{}

	r.KeySize = byteOrdering.Uint32(contents)
	offset := 4
	r.Key = contents[offset : offset+int(r.KeySize)]
	offset += int(r.KeySize)
	r.ValueSize = byteOrdering.Uint32(contents[offset:])
	offset += 4
	r.Value = contents[offset : offset+int(r.ValueSize)]
	offset += int(r.ValueSize)
	r.AtomicCount = byteOrdering.Uint64(contents[offset:])

	return r
}

func (r *Record) Size() int {
	return 4 + int(r.KeySize) + 4 + int(r.ValueSize) + 8
}

func KeyFromDisk(r *os.File, offset int64) (*Record, error) {
	keySizBytes := make([]byte, 4)
	_, err := r.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	if _, err := r.Read(keySizBytes); err != nil {
		return nil, err
	}
	offset += 4
	keySize := byteOrdering.Uint32(keySizBytes)
	key := make([]byte, keySize)
	if _, err := r.ReadAt(key, offset); err != nil {
		return nil, err
	}
	return &Record{KeySize: keySize, Key: key}, nil
}

func RecordFromDisk(r *os.File, offset int64) (*Record, error) {
	rec := &Record{}
	keySizBytes := make([]byte, 4)
	_, err := r.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	if _, err := r.Read(keySizBytes); err != nil {
		return nil, err
	}
	offset += 4
	rec.KeySize = byteOrdering.Uint32(keySizBytes)
	rec.Key = make([]byte, rec.KeySize)
	if _, err := r.ReadAt(rec.Key, offset); err != nil {
		return nil, err
	}
	offset += int64(rec.KeySize)
	valueSizeBytes := make([]byte, 4)
	if _, err := r.ReadAt(valueSizeBytes, offset); err != nil {
		return nil, err
	}
	offset += 4
	rec.ValueSize = byteOrdering.Uint32(valueSizeBytes)
	rec.Value = make([]byte, rec.ValueSize)
	if _, err := r.ReadAt(rec.Value, offset); err != nil {
		return nil, err
	}
	offset += int64(rec.ValueSize)

	atomicCountBytes := make([]byte, 8)
	if _, err := r.ReadAt(atomicCountBytes, offset); err != nil {
		return nil, err
	}
	rec.AtomicCount = byteOrdering.Uint64(atomicCountBytes)
	return rec, nil
}
