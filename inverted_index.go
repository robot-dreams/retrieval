package retrieval

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

var nullByte = byte(0)

type InvertedIndex struct {
	db *leveldb.DB
}

var _ Index = (*InvertedIndex)(nil)

func OpenInvertedIndex(location string) (*InvertedIndex, error) {
	db, err := leveldb.OpenFile(location, nil)
	if err != nil {
		return nil, err
	}
	return &InvertedIndex{
		db: db,
	}, nil
}

func (ii *InvertedIndex) Add(filename string, contents string) error {
	tokens := tokenize(contents)
	for _, token := range tokens {
		key := joinWithNullSep(token, filename)
		err := ii.db.Put(key, nil, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ii *InvertedIndex) Search(query string) ([]string, error) {
	prefix := append([]byte(query), nullByte)
	result := make([]string, 0)
	iterFunc := func(key []byte, value []byte) error {
		sep := bytes.IndexByte(key, nullByte)
		if sep < 0 {
			errorMessage := fmt.Sprintf(
				"Invalid key format: %q; possible index corruption?",
				key)
			return errors.New(errorMessage)
		}
		filename := string(key[sep+1:])
		result = append(result, filename)
		return nil
	}
	err := iterate(ii.db, prefix, iterFunc)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (ii *InvertedIndex) Close() error {
	return ii.db.Close()
}
