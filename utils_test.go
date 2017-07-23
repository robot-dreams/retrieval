package retrieval

import (
	"reflect"
	"testing"
)

func TestTokenize(t *testing.T) {
	for _, testCase := range []struct {
		input    string
		expected []string
	}{
		{"", []string{""}},
		{"a", []string{"a"}},
		{"a b", []string{"a", "b"}},
		{"a b	c\td\ne", []string{"a", "b", "c", "d", "e"}},
	} {
		output := tokenize(testCase.input)
		if !reflect.DeepEqual(testCase.expected, output) {
			t.Errorf(
				"Expected %q for input %q; got %q",
				testCase.expected,
				testCase.input,
				output)
		}
	}
}
