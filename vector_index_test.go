package retrieval

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestVectorIndexDFCache(t *testing.T) {
	// Initialization
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

	location := filepath.Join(tempDir, "v")
	v, err := OpenVectorIndex(location)
	if err != nil {
		t.Error(err)
	}

	err = v.Add("file.txt", "foo foo bar baz")
	if err != nil {
		t.Error(err)
	}
	expected := map[string]uint32{
		"foo": 2,
		"bar": 1,
		"baz": 1,
	}
	if !reflect.DeepEqual(expected, v.dfCache) {
		t.Errorf(
			"Expected %v; got %v",
			expected,
			v.dfCache)
	}
	err = v.Add("file2.txt", "bar bar foo foo bat")
	if err != nil {
		t.Error(err)
	}
	expected = map[string]uint32{
		"foo": 4,
		"bar": 3,
		"baz": 1,
		"bat": 1,
	}
	if !reflect.DeepEqual(expected, v.dfCache) {
		t.Errorf(
			"Expected %v; got %v",
			expected,
			v.dfCache)
	}
	err = v.Close()
	if err != nil {
		t.Error(err)
	}
	v2, err := OpenVectorIndex(location)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(expected, v2.dfCache) {
		t.Errorf(
			"Expected %v; got %v",
			expected,
			v.dfCache)
	}
	err = v2.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestVectorIndexIncrement(t *testing.T) {
	// Initialization
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

	location := filepath.Join(tempDir, "v")
	v, err := OpenVectorIndex(location)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := v.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	key := []byte{1, 2, 3, 4}
	err = v.increment(key)
	if err != nil {
		t.Error(err)
	}
	expected := []byte{0, 0, 0, 1}
	actual, err := v.db.Get(key, nil)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf(
			"Expected %v for key %v; got %v",
			expected,
			key,
			actual)
	}
	err = v.increment(key)
	if err != nil {
		t.Error(err)
	}
	expected = []byte{0, 0, 0, 2}
	actual, err = v.db.Get(key, nil)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf(
			"Expected %v for key %v; got %v",
			expected,
			key,
			actual)
	}
}

func TestVectorIndexVectors(t *testing.T) {
	// Initialization
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

	location := filepath.Join(tempDir, "v")
	v, err := OpenVectorIndex(location)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := v.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	err = v.Add("file.txt", "foo bar baz")
	if err != nil {
		t.Error(err)
	}
	actual, err := v.documentVector("file.txt")
	if err != nil {
		t.Error(err)
	}
	expected := map[string]float64{
		"foo": 1,
		"bar": 1,
		"baz": 1,
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf(
			"Expected %v; got %v",
			expected,
			actual)
	}
	err = v.Add("file2.txt", "foo bar baz")
	if err != nil {
		t.Error(err)
	}
	actual, err = v.documentVector("file.txt")
	expected = map[string]float64{
		"foo": 0.5,
		"bar": 0.5,
		"baz": 0.5,
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf(
			"Expected %v; got %v",
			expected,
			actual)
	}
}

func TestVectorIndex(t *testing.T) {
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

	location := filepath.Join(tempDir, "v")
	v, err := OpenVectorIndex(location)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := v.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	for _, input := range []struct {
		filename string
		contents string
	}{
		{"a", "hello hello hello hello world"},
		{"b", "hello tiger tiger"},
		{"c", "rumic tiger"},
	} {
		err := v.Add(input.filename, input.contents)
		if err != nil {
			t.Error(err)
		}
	}

	for _, testCase := range []struct {
		query    string
		expected []string
	}{
		{"hello", []string{"a", "b"}},
		{"tiger", []string{"b", "c"}},
		{"world", []string{"a"}},
	} {
		result, err := v.Search(testCase.query)
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
