package retrieval

import (
	"log"
	"regexp"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var whitespace = regexp.MustCompile("\\s+")

func tokenize(input string) []string {
	return whitespace.Split(input, -1)
}

func joinWithNullSep(token string, filename string) []byte {
	result := make([]byte, 0, len(token)+1+len(filename))
	result = append(result, []byte(token)...)
	result = append(result, nullByte)
	result = append(result, []byte(filename)...)
	return result
}

func iterate(
	db *leveldb.DB,
	prefix []byte,
	iterFunc func([]byte, []byte) error) error {

	prefixRange := util.BytesPrefix(prefix)
	iter := db.NewIterator(prefixRange, nil)
	defer func() {
		iter.Release()
		if err := iter.Error(); err != nil {
			log.Print(err)
		}
	}()

	for iter.Next() {
		err := iterFunc(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
	}
	return nil
}
