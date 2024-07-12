package sstable

type Memtable interface {
	Searcher
	Insert(key, value []byte, atomicCount uint64)
	ToSSTable(tableName string) *SSTable
}
