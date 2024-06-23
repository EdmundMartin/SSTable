package sstable

type Serializable interface {
	ToBytes() ([]byte, error)
	Size() int
}
