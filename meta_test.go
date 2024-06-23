package sstable

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewTableMeta(t *testing.T) {

	res := NewTableMeta("hello_test", 3)

	contents, err := res.ToBytes()
	assert.NoError(t, err)

	result := TableMetaFromBytes(contents)
	assert.Equal(t, res.TableName, result.TableName)
	assert.Equal(t, res.KeyCount, result.KeyCount)
	assert.Equal(t, res.Offsets, result.Offsets)
}
