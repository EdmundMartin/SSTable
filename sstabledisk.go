package sstable

import (
	"bytes"
	"encoding/binary"
	"os"
)

// DiskTable provides a way to interact with a file based table. Supporting search operation over the file.
type DiskTable struct {
	file      *os.File
	TableMeta *TableMeta
}

func NewDiskTable(fileName string) (*DiskTable, error) {

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	var metaSize uint32
	err = binary.Read(file, byteOrdering, &metaSize)
	if err != nil {
		return nil, err
	}

	metaBytes := make([]byte, metaSize)
	if _, err := file.ReadAt(metaBytes, 0); err != nil {
		return nil, err
	}

	tableMeta := TableMetaFromBytes(metaBytes)

	return &DiskTable{
		file:      file,
		TableMeta: tableMeta,
	}, nil
}

func (d *DiskTable) binarySearch(key []byte) (*Record, error) {

	var low uint32
	high := d.TableMeta.KeyCount - 1
	for low <= high {
		middle := (low + high) / 2

		offset := d.TableMeta.Offsets[middle]

		otherKey, err := KeyFromDisk(d.file, int64(offset))
		if err != nil {
			return nil, err
		}
		cmp := bytes.Compare(key, otherKey.Key)
		if cmp == 0 {
			return RecordFromDisk(d.file, int64(offset))
		}
		if cmp < 0 {
			high = middle - 1
		} else {
			low = middle + 1
		}
	}
	return nil, nil
}

func (d *DiskTable) Contains(key []byte) (bool, error) {
	val, err := d.binarySearch(key)
	if val.Deleted() {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

func (d *DiskTable) Get(key []byte) (*Record, error) {
	val, err := d.binarySearch(key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	if val.Deleted() {
		return nil, nil
	}
	return val, nil
}

func (d *DiskTable) Scan() ([]*Record, error) {
	results := make([]*Record, 0, d.TableMeta.KeyCount)
	for i := 0; i < int(d.TableMeta.KeyCount); i++ {
		offset := d.TableMeta.Offsets[i]
		rec, err := RecordFromDisk(d.file, int64(offset))
		if err != nil {
			return nil, err
		}
		if !rec.Deleted() {
			results = append(results, rec)
		}
	}
	return results, nil
}

func (d *DiskTable) ScanWithLimit(limit *Limit) ([]*Record, error) {
	resultSize := min(d.TableMeta.KeyCount, uint32(limit.MaxResults))
	results := make([]*Record, 0, resultSize)

	for i := 0; i < int(d.TableMeta.KeyCount); i++ {
		if len(results) == int(resultSize) {
			return results, nil
		}
		offset := d.TableMeta.Offsets[i]
		rec, err := RecordFromDisk(d.file, int64(offset))
		if err != nil {
			return nil, err
		}
		if !rec.Deleted() {
			results = append(results, rec)
		}
	}
	return results, nil
}

func (d *DiskTable) ScanWithPredicate(pred Predicate, limit *Limit) ([]*Record, error) {
	resultSize := min(d.TableMeta.KeyCount, uint32(limit.MaxResults))
	results := make([]*Record, 0, resultSize)

	for i := 0; i < int(d.TableMeta.KeyCount); i++ {
		if len(results) == int(resultSize) {
			return results, nil
		}
		offset := d.TableMeta.Offsets[i]
		rec, err := RecordFromDisk(d.file, int64(offset))
		if err != nil {
			return nil, err
		}
		if !rec.Deleted() && pred(rec.Key, rec.Value) {
			results = append(results, rec)
		}
	}
	return results, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}