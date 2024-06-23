package sstable

type Limit struct {
	MaxResults uint64
}

type Predicate func(key, value []byte) bool

type Searcher interface {
	Contains(key []byte) (bool, error)
	Get(key []byte) (*Record, error)
	Scan() ([]*Record, error)
	ScanWithLimit(limit *Limit) ([]*Record, error)
	ScanWithPredicate(pred Predicate, limit *Limit) ([]*Record, error)
}
