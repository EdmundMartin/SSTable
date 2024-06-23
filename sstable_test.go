package sstable

import (
	"fmt"
	"os"
	"testing"
)

func removeFile(fileName string) {
	os.Remove(fileName)
}

func TestNewSSTable(t *testing.T) {
	nameForTest := "new_test"
	defer removeFile(nameForTest)

	recs := []*Record{
		{
			KeySize:     1,
			Key:         []byte("A"),
			ValueSize:   1,
			Value:       []byte("A"),
			AtomicCount: 1,
		},
		{
			KeySize:     1,
			Key:         []byte("B"),
			ValueSize:   1,
			Value:       []byte("B"),
			AtomicCount: 2,
		},
		{
			KeySize:     1,
			Key:         []byte("C"),
			ValueSize:   1,
			Value:       []byte("C"),
			AtomicCount: 1,
		},
	}

	table := NewSSTable("test_table", recs)
	if err := table.SaveToDisk(func(tableName string) string {
		return nameForTest
	}); err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	fromDisk, err := NewDiskTable(nameForTest)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	r, err := fromDisk.binarySearch([]byte("A"))
	fmt.Println(r)
	fmt.Println(err)
}
