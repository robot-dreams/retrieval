package retrieval

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

// We consider documents (and queries) to be elements of an N-dimensional vector
// space, where each dimension corresponds to a term in the corpus.  For a given
// document (or query), the value for a dimension is the TF * IDF of the
// corresponding term.
//
// We define the "similarity" of two vectors as the cosine of the angle between
// them.
//
// Given a query, we treat it as a vector and then return documents in
// decreasing order of similarity to the query vector.
//
// Implementation details:
// - We use binary.BigEndian to encode integers, and we always use uint32
// - We use filenames to refer to documents
// - We use a single DB but different prefixes for each mapping
// - We assume Add is only ever called from a single goroutine

// filename\0term -> number of time a document contains a term
// Note that this implies filenames cannot contain '\0'
const ftPrefix = "ft"

// term -> number of times a term appears in the corpus
const dfPrefix = "df"

// term\0filename -> number of time a term appears in a document
// Note that this implies terms cannot contain '\0'
const tfPrefix = "tf"

type VectorIndex struct {
	sync.Mutex
	db      *leveldb.DB
	dfCache map[string]uint32
}

var _ Index = (*VectorIndex)(nil)

func loadDFCache(db *leveldb.DB) (map[string]uint32, error) {
	dfCache := make(map[string]uint32)
	iterFunc := func(key []byte, value []byte) error {
		term := string(key[len(dfPrefix):])
		if len(value) != 4 {
			errorMessage := fmt.Sprintf(
				"Key %v has invalid encoding of uint32 value: %v",
				key,
				value)
			return errors.New(errorMessage)
		}
		dfCache[term] = binary.BigEndian.Uint32(value)
		return nil
	}
	err := iterate(db, []byte(dfPrefix), iterFunc)
	if err != nil {
		return nil, err
	}
	return dfCache, nil
}

func OpenVectorIndex(location string) (*VectorIndex, error) {
	db, err := leveldb.OpenFile(location, nil)
	if err != nil {
		return nil, err
	}

	dfCache, err := loadDFCache(db)
	return &VectorIndex{
		db:      db,
		dfCache: dfCache,
	}, nil
}

func (v *VectorIndex) increment(keyBytes []byte) error {
	has, err := v.db.Has(keyBytes, nil)
	if !has {
		valueBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(valueBytes, 1)
		return v.db.Put(keyBytes, valueBytes, nil)
	}
	valueBytes, err := v.db.Get(keyBytes, nil)
	if err != nil {
		return err
	}

	var value uint32
	if len(valueBytes) == 0 {
		value = 0
	} else if len(valueBytes) == 4 {
		value = binary.BigEndian.Uint32(valueBytes)
	} else {
		errorMessage := fmt.Sprintf(
			"Key %v has invalid encoding of uint32 value: %v",
			keyBytes,
			valueBytes)
		return errors.New(errorMessage)
	}

	if value == math.MaxUint32 {
		errorMessage := fmt.Sprintf(
			"Key %v has value %d and cannot be incremented anymore",
			keyBytes,
			value)
		return errors.New(errorMessage)
	}
	binary.BigEndian.PutUint32(valueBytes, value+1)
	return v.db.Put(keyBytes, valueBytes, nil)
}

func (v *VectorIndex) Add(filename string, contents string) error {
	tokens := tokenize(contents)
	for _, token := range tokens {
		ftKey := []byte(ftPrefix)
		ftKey = append(ftKey, joinWithNullSep(filename, token)...)

		dfKey := []byte(dfPrefix)
		dfKey = append(dfKey, []byte(token)...)

		tfKey := []byte(tfPrefix)
		tfKey = append(tfKey, joinWithNullSep(token, filename)...)

		for _, keyBytes := range [][]byte{ftKey, dfKey, tfKey} {
			err := v.increment(keyBytes)
			if err != nil {
				return err
			}
		}
		v.dfCache[token]++
	}
	return nil
}

func (v *VectorIndex) documentVector(
	filename string) (map[string]float64, error) {

	prefix := []byte(ftPrefix)
	prefix = append(prefix, []byte(filename)...)
	prefix = append(prefix, nullByte)
	result := make(map[string]float64)
	iterFunc := func(key []byte, value []byte) error {
		term := string(key[len(prefix):])
		if len(value) != 4 {
			errorMessage := fmt.Sprintf(
				"Key %v has invalid encoding of uint32 value: %v",
				key,
				value)
			return errors.New(errorMessage)
		}
		tf := binary.BigEndian.Uint32(value)
		df := v.dfCache[term]
		result[term] = float64(tf) / float64(df)
		return nil
	}
	err := iterate(v.db, prefix, iterFunc)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (v *VectorIndex) queryVector(
	query string) map[string]float64 {

	queryTF := make(map[string]uint32)
	for _, token := range tokenize(query) {
		queryTF[token]++
	}
	result := make(map[string]float64)
	for term, tf := range queryTF {
		df, ok := v.dfCache[term]
		if ok {
			result[term] = float64(tf) / float64(df)
		}
	}
	return result
}

func dotProduct(x map[string]float64, y map[string]float64) float64 {
	result := float64(0)
	for term, tfIDF := range x {
		result += tfIDF * y[term]
	}
	return result
}

func magnitude(vector map[string]float64) float64 {
	result := float64(0)
	for _, tfIDF := range vector {
		result += math.Pow(tfIDF, 2)
	}
	return result
}

func (v *VectorIndex) queryDocumentSimilarity(
	query string,
	filename string) (float64, error) {

	q := v.queryVector(query)
	d, err := v.documentVector(filename)
	if err != nil {
		return 0, err
	}
	return dotProduct(q, d) / (magnitude(q) * magnitude(d)), nil
}

func uniqueTerms(query string) map[string]struct{} {
	result := make(map[string]struct{})
	for _, token := range tokenize(query) {
		result[token] = struct{}{}
	}
	return result
}

type scoredCandidate struct {
	candidate string
	score     float64
}

type byScore []*scoredCandidate

func (s byScore) Len() int           { return len(s) }
func (s byScore) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byScore) Less(i, j int) bool { return s[i].score > s[j].score }

func (v *VectorIndex) Search(query string) ([]string, error) {
	candidates := make(map[string]struct{})

	for term, _ := range uniqueTerms(query) {
		prefix := []byte(tfPrefix)
		prefix = append(prefix, []byte(term)...)
		prefix = append(prefix, nullByte)
		iterFunc := func(key []byte, value []byte) error {
			sep := bytes.IndexByte(key, nullByte)
			if sep < 0 {
				errorMessage := fmt.Sprintf(
					"Invalid key format: %q; possible index corruption?",
					key)
				return errors.New(errorMessage)
			}
			filename := string(key[sep+1:])
			candidates[filename] = struct{}{}
			return nil
		}
		err := iterate(v.db, prefix, iterFunc)
		if err != nil {
			return nil, err
		}
	}

	scoredCandidates := make([]*scoredCandidate, 0, len(candidates))
	for candidate, _ := range candidates {
		score, err := v.queryDocumentSimilarity(query, candidate)
		if err != nil {
			return nil, err
		}
		scoredCandidates = append(
			scoredCandidates,
			&scoredCandidate{
				score:     score,
				candidate: candidate,
			})
	}
	sort.Sort(byScore(scoredCandidates))
	result := make([]string, len(scoredCandidates))
	for i, scoredCandidate := range scoredCandidates {
		result[i] = scoredCandidate.candidate
	}
	return result, nil
}

func (v *VectorIndex) Close() error {
	return v.db.Close()
}
