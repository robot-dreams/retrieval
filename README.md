# retrieval

This repository is a library containing a few experiments in text retrieval:

- Backing a search index using a log-structured merge tree (i.e. LevelDB)
- Storing an inverted index in an "exploded" format
    - `token | docID -> (null)` as opposed to `token -> [list of docID]`
- Vector-space model for documents, along with BM25 scoring

Nothing here is field-tested or production ready; it's just scratch work for educational purposes.
