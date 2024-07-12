package sstable

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBst_Contains(t *testing.T) {

	bst := NewBst()

	bst.Insert([]byte("Hello"), []byte("World"), 1)
	bst.Insert([]byte("Example"), []byte("Value"), 2)
	bst.Insert([]byte("Roger"), []byte("King"), 3)

	all, _ := bst.Scan()
	fmt.Println(all)

	ok, _ := bst.Contains([]byte("Hello"))
	assert.True(t, ok)

	ok, _ = bst.Contains([]byte("Example"))
	assert.True(t, ok)

	ok, _ = bst.Contains([]byte("Roger"))
	assert.True(t, ok)

	ok, _ = bst.Contains([]byte("Not found"))
	assert.False(t, ok)
}
