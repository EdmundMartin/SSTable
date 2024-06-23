package sstable

import "bytes"

func MergeTables(first, second *SSTable) *SSTable {
	mapCapacity := len(first.Records) + len(second.Records)
	mapping := make(map[string]*Record, mapCapacity)
	for _, rec := range first.Records {
		key := string(rec.Key)
		val, ok := mapping[key]
		if !ok {
			mapping[key] = rec
		} else {
			if val.AtomicCount < rec.AtomicCount {
				mapping[key] = rec
			}
		}
	}
	newRecordSet := make([]*Record, 0, len(mapping))
	for _, val := range mapping {
		// filter out deleted records from the new SSTable
		if bytes.Compare(val.Value, TombstoneMarker) != 0 {
			newRecordSet = append(newRecordSet, val)
		}
	}
	return NewSSTable(string(first.Metadata.TableName), newRecordSet)
}
