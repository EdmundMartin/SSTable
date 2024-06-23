package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	stats, _ := d.file.Stat()
	fmt.Println(stats)
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
	if bytes.Compare(val.Value, TombstoneMarker) == 0 {
		return nil, nil
	}
	return val, nil
}
