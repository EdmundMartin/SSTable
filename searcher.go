package sstable

type Searcher interface {
	Contains(key []byte) (bool, error)
	Get(key []byte) (*Record, error)
	Lt(key []byte) ([]*Record, error)
	Gt(key []byte) ([]*Record, error)
	Scan() ([]*Record, error)
}
