package retrieval

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestInvertedIndex(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := os.RemoveAll(tempDir)
		if err != nil {
			t.Error(err)
		}
	}()

	location := filepath.Join(tempDir, "ii")
	ii, err := OpenInvertedIndex(location)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := ii.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	for _, input := range []struct {
		filename string
		contents string
	}{
		{"a", "hello world"},
		{"b", "hello tiger"},
		{"c", "rumic world"},
	} {
		err := ii.Add(input.filename, input.contents)
		if err != nil {
			t.Error(err)
		}
	}

	for _, testCase := range []struct {
		query    string
		expected []string
	}{
		{"hello", []string{"a", "b"}},
		{"tiger", []string{"b"}},
		{"world", []string{"a", "c"}},
	} {
		result, err := ii.Search(testCase.query)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(testCase.expected, result) {
			t.Errorf(
				"Expected %q for query %q; got %q",
				testCase.expected,
				testCase.query,
				result)
		}
	}
}
