package sstable

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"sync/atomic"
	"testing"
)

func TestDiskTable_Contains(t *testing.T) {
	testTableName := "TestDiskTableContains"
	contents := [][]byte{
		[]byte("A"),
		[]byte("B"),
		[]byte("C"),
		[]byte("D"),
		[]byte("E"),
		[]byte("F"),
	}
	notFound := [][]byte{
		[]byte("AA"),
		[]byte("BB"),
		[]byte("Z"),
		[]byte("G"),
	}

	var allRecords []*Record

	var counter uint64

	countFunc := func() uint64 {
		return atomic.AddUint64(&counter, 1)
	}

	for _, content := range contents {
		allRecords = append(allRecords, NewRecord(content, content, countFunc))
	}

	table := NewSSTable("ExampleTest", allRecords)
	err := table.SaveToDisk(func(tableName string) string {
		return testTableName
	})
	require.NoError(t, err)
	defer os.Remove(testTableName)

	diskTable, err := NewDiskTable(testTableName)
	assert.NoError(t, err)

	for _, key := range contents {
		found, err := diskTable.Contains(key)
		assert.True(t, found)
		assert.NoError(t, err)
	}

	for _, key := range notFound {
		found, err := diskTable.Contains(key)
		assert.False(t, found)
		assert.NoError(t, err)
	}
}
