package sstable

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"math"
	"os"
	"sort"
)

// SSTable is a struct which represents the structure of an SSTable stored in memory
type SSTable struct {
	Metadata *TableMeta
	Records  Records
}

func (s *SSTable) binarySearch(key []byte) (*Record, error) {
	var low uint32
	high := s.Metadata.KeyCount - 1
	for low <= high {
		middle := (low + high) / 2

		cmp := bytes.Compare(key, s.Records[middle].Key)
		if cmp == 0 {
			return s.Records[middle], nil
		}
		if cmp < 0 {
			high = middle - 1
		} else {
			low = middle + 1
		}
	}
	return nil, nil
}

func (s *SSTable) Contains(key []byte) (bool, error) {
	result, _ := s.binarySearch(key)
	return result != nil, nil
}

func (s *SSTable) Get(key []byte) (*Record, error) {
	return s.binarySearch(key)
}

func (s *SSTable) Scan() ([]*Record, error) {
	return s.Records, nil
}

func (s *SSTable) ScanWithLimit(limit *Limit) ([]*Record, error) {
	if limit == nil {
		return s.Records, nil
	}
	return s.Records[:limit.MaxResults], nil
}

func (s *SSTable) ScanWithPredicate(pred Predicate, limit *Limit) ([]*Record, error) {
	limitValue := math.MaxInt
	if limit != nil {
		limitValue = int(limit.MaxResults)
	}
	results := make([]*Record, 0, s.Metadata.KeyCount)
	count := 0
	for _, rec := range s.Records {

		if pred(rec.Key, rec.Value) && count < limitValue {
			results = append(results, rec)
			count += 1
		}

		if count == limitValue {
			return results, nil
		}
	}
	return results, nil
}

type TableNameFunc func(tableName string) string

func (s *SSTable) SaveToDisk(nameFunc TableNameFunc) error {
	var filename string
	if nameFunc == nil {
		v4uuid := uuid.New()
		filename = fmt.Sprintf("%s-%s", s.Metadata.TableName, v4uuid.String())
	} else {
		filename = nameFunc(string(s.Metadata.TableName))
	}

	f, err := os.Create(filename)
	defer f.Close()
	if err != nil {
		return err
	}
	contents, err := s.ToBytes()
	if err != nil {
		return err
	}
	if _, err := f.Write(contents); err != nil {
		return err
	}
	return nil
}

func (s *SSTable) ToBytes() ([]byte, error) {
	buffer := make([]byte, s.Size())
	offset := 0

	metaBytes, err := s.Metadata.ToBytes()
	if err != nil {
		return nil, err
	}
	copy(buffer[offset:], metaBytes)
	offset += len(metaBytes)

	recordBytes, err := s.Records.ToBytes()
	if err != nil {
		return nil, err
	}
	copy(buffer[offset:], recordBytes)
	return buffer, nil
}

func (s *SSTable) Size() int {
	return s.Metadata.Size() + s.Records.Size()
}

func NewSSTable(tableName string, records []*Record) *SSTable {
	metadata := NewTableMeta(tableName, uint32(len(records)))
	offset := metadata.Size()

	sort.Sort(Records(records))
	for idx, record := range records {
		metadata.Offsets[idx] = uint32(offset)
		offset += record.Size()
	}

	return &SSTable{
		Metadata: metadata,
		Records:  records,
	}
}
