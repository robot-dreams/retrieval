package retrieval

import (
	"bytes"
	"errors"
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var nullByte = byte(0)

type InvertedIndex struct {
	db *leveldb.DB
}

var _ Index = (*InvertedIndex)(nil)

func makeKey(token string, filename string) []byte {
	result := make([]byte, 0, len(token)+1+len(filename))
	result = append(result, []byte(token)...)
	result = append(result, nullByte)
	result = append(result, []byte(filename)...)
	return result
}

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
		key := makeKey(token, filename)
		err := ii.db.Put(key, nil, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ii *InvertedIndex) Search(query string) ([]string, error) {
	prefix := []byte(query)
	prefix = append(prefix, nullByte)
	prefixRange := util.BytesPrefix(prefix)

	iter := ii.db.NewIterator(prefixRange, nil)
	defer func() {
		iter.Release()
		if err := iter.Error(); err != nil {
			log.Print(err)
		}
	}()

	result := make([]string, 0)
	for iter.Next() {
		key := iter.Key()
		sep := bytes.IndexByte(key, nullByte)
		if sep < 0 {
			errorMessage := fmt.Sprintf(
				"Invalid key format: %q; possible index corruption?",
				key)
			return nil, errors.New(errorMessage)
		}
		filename := string(key[sep+1:])
		result = append(result, filename)
	}
	return result, nil
}

func (ii *InvertedIndex) Close() error {
	return ii.db.Close()
}
