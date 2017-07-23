package retrieval

type Index interface {
	Add(filename string, contents string) error
	Search(query string) ([]string, error)
	Close() error
}
