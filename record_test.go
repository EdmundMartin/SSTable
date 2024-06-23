package sstable

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestNewRecord(t *testing.T) {

	record := &Record{
		KeySize:     5,
		Key:         []byte("Hello"),
		ValueSize:   5,
		Value:       []byte("World"),
		AtomicCount: 1,
	}

	rec := NewRecord([]byte("Hello"), []byte("World"), func() uint64 {
		return 1
	})

	assert.Equal(t, record, rec)
}

func TestRecord_ToBytes(t *testing.T) {
	record := &Record{
		KeySize:     5,
		Key:         []byte("Hello"),
		ValueSize:   5,
		Value:       []byte("World"),
		AtomicCount: 1,
	}

	byteRecord, err := record.ToBytes()
	assert.NoError(t, err)

	otherRecord := RecordFromBytes(byteRecord)
	assert.Equal(t, record, otherRecord)
}

func TestKeyFromDisk(t *testing.T) {
	testFileName := "keyFromDisk"
	record := &Record{
		KeySize:     5,
		Key:         []byte("Hello"),
		ValueSize:   5,
		Value:       []byte("World"),
		AtomicCount: 1,
	}
	contents, err := record.ToBytes()
	require.NoError(t, err)

	file, err := os.Create(testFileName)
	require.NoError(t, err)
	defer os.Remove(testFileName)

	_, err = file.Write(contents)
	require.NoError(t, err)

	res, err := KeyFromDisk(file, 0)
	assert.Equal(t, uint32(5), res.KeySize)
	assert.Equal(t, []byte("Hello"), res.Key)
	assert.NoError(t, err)
}

func TestRecordFromDisk(t *testing.T) {
	testFileName := "recordFromDisk"
	record := &Record{
		KeySize:     5,
		Key:         []byte("Hello"),
		ValueSize:   5,
		Value:       []byte("World"),
		AtomicCount: 174,
	}
	contents, err := record.ToBytes()
	file, err := os.Create(testFileName)
	require.NoError(t, err)
	defer os.Remove(testFileName)

	_, err = file.Write(contents)
	require.NoError(t, err)

	res, err := RecordFromDisk(file, 0)
	assert.Equal(t, uint32(5), res.KeySize)
	assert.Equal(t, []byte("Hello"), res.Key)
	assert.Equal(t, uint32(5), res.ValueSize)
	assert.Equal(t, []byte("World"), res.Value)
	assert.Equal(t, uint64(174), res.AtomicCount)
	assert.NoError(t, err)
}
