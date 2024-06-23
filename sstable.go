package sstable

import (
	"fmt"
	"os"
	"sort"
)

type SSTable struct {
	Metadata *TableMeta
	Records  Records
}

type TableNameFunc func(tableName string) string

func (s *SSTable) SaveToDisk(nameFunc TableNameFunc) error {

	// TODO - Implement UUID here
	var filename string
	if nameFunc == nil {
		filename = fmt.Sprintf("%s-%s", s.Metadata.TableName, "lol")
	} else {
		filename = nameFunc(string(s.Metadata.TableName))
	}

	f, err := os.Create(filename)
	defer f.Close()
	if err != nil {
		return err
	}
	contents, err := s.ToBytes()
	fmt.Println(contents)
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

