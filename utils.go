package retrieval

import "regexp"

var whitespace = regexp.MustCompile("\\s+")

func tokenize(input string) []string {
	return whitespace.Split(input, -1)
}
