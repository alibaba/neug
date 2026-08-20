# Full-Text Search Extension

Since NeuG **v0.2.0**, the `fts` extension provides full-text indexes and
BM25-ranked search over node string properties, with automatic index
maintenance and persistence.

The FTS extension supports:

- Full-text indexes on node properties of type `STRING`
- Word, phrase, prefix, Boolean, and exclusion queries
- Top-K retrieval ordered by BM25 relevance
- Support scalar filtering and graph filtering
- Support pre-filtering and post-filtering with exact Top-K results
- Automatic maintenance after inserts, updates, and deletes
- Index checkpointing and recovery

## Install and Load the Extension

Install and load the extension before creating or querying an FTS index:

```cypher
INSTALL fts;
LOAD fts;
```

## Create an FTS Index

An FTS index can be created on one `STRING` property of a node table with the
unified `CREATE INDEX` syntax:

```cypher
CREATE INDEX <index_name> [IF NOT EXISTS]
ON <node_table>
USING FTS (<string_property>)
[WITH (
    tokenizer = '<tokenizer>',
    prefix = '<prefix_lengths>',
    detail = '<detail_mode>'
)];
```

The `WITH` clause and each option in it are optional. For example, the
following statements create a node table and an FTS index with the default
options:

```cypher
CREATE NODE TABLE Article (
    id INT64 PRIMARY KEY,
    title STRING,
    category STRING
);

CREATE INDEX article_title_fts
ON Article
USING FTS (title);
```

In this statement:

- `article_title_fts` is the index name. It must start with a letter or
  underscore and contain only letters, digits, or underscores.
- `Article` is the node table containing the indexed property.
- `FTS` selects the full-text index type.
- `title` is the `STRING` property to index.

Existing nodes are indexed when the index is created. Subsequent inserts,
updates, and deletes automatically update its contents.

### Index Options

The `WITH` clause accepts the following case-sensitive option names:

| Option | Description | Default |
| --- | --- | --- |
| `tokenizer` | Tokenization strategy used to split indexed text into searchable terms | `unicode61` |
| `prefix` | Space-separated token lengths for prefix indexes, such as `2 3` | No prefix index |
| `detail` | Match-detail mode: `full`, `column`, or `none` | `full` |

Supported tokenizers are:

- `unicode61` splits text according to Unicode 6.1 rules. This is the default.
- `ascii` applies ASCII tokenization rules. Characters outside the ASCII range
  (0-127) are treated as token characters.
- `porter` applies the Porter stemming algorithm so related word forms can
  match the same stem.
- `trigram` treats each contiguous sequence of three characters as a token,
  enabling substring matching.

The `detail` option affects the query forms available to users:

- `none` records only the row ID for each term.
- `column` records the row ID and column for each term, allowing queries to
  filter matches by column. (NeuG does not currently support multi-property FTS
  indexes.)
- `full` records the row ID, column, and term offset for each term, additionally
  supporting phrase queries. This is the default.

For example, the Porter tokenizer can be combined with `unicode61`, and prefix
indexes can be created for two- and three-character prefixes:

```cypher
CREATE INDEX article_title_fts
ON Article
USING FTS (title)
WITH (
    tokenizer = 'porter unicode61',
    prefix = '2 3',
    detail = 'full'
);
```

Invalid tokenizer, prefix, or detail settings cause index creation to fail.
The selected settings cannot be changed in place; drop and recreate the index
to use different settings.

## Inspect and Drop an FTS Index

Use `SHOW_INDEXES()` to inspect indexes:

```cypher
CALL SHOW_INDEXES() RETURN *;
```

For the default index created above, the result includes an entry similar to:

| name | type | label | property | options |
| --- | --- | --- | --- | --- |
| `article_title_fts` | `fts` | `Article` | `title` | `{}` |

Drop the index by its name:

```cypher
DROP INDEX article_title_fts IF EXISTS;
```

Dropping the indexed property or its node table also removes the associated
index.

## Full-Text Search

Use `bm25(indexed_property, query)` in a Top-K query. BM25 scores use smaller
values for more relevant matches. The FTS index returns matches in ascending
BM25 score order by default; a typical Top-K query makes that order explicit:

```cypher
MATCH (article:Article)
RETURN article.id,
       article.title,
       bm25(article.title, 'graph database') AS score
ORDER BY score ASC
LIMIT 10;
```

When an FTS index exists for `Article.title`, the query returns both the
matching node and its BM25 score.

Note:

1. Without `ORDER BY`, results are sorted by BM25 score in ascending order by
   default. Without `LIMIT`, all matching results are returned.
2. BM25 scores can be ordered with either `ASC` or `DESC`, with an optional
   `LIMIT`.
3. `ORDER BY` and `LIMIT` can also be applied to another returned column or
   expression.

### Query Syntax

The second argument of `bm25` contains a full-text query. When supplied as a
string literal, common forms include:

| Search type | Query string | Meaning |
| --- | --- | --- |
| Word | `'database'` | Match the token `database` |
| Multiple terms | `'graph database'` | Match documents containing both terms |
| Phrase | `'"graph database"'` | Match adjacent terms in the specified order |
| Prefix | `'data*'` | Match tokens beginning with `data` |
| Boolean | `'graph OR database'` | Match either term |
| Exclusion | `'graph NOT database'` | Match `graph` without `database` |

Punctuation that is not valid in an unquoted query term must be included in a
phrase. For example, use `'"DLF-Legacy"'` instead of `'DLF-Legacy'`.
Unterminated quotes, empty queries, and invalid query syntax return a query
execution error. Available tokenization behavior depends on the `tokenizer`
selected when the index is created.

### Dynamic Query Parameters

The second argument of `bm25` can also be a dynamic `STRING` parameter. This
allows an application to reuse the same statement with a different full-text
query on each execution:

```cypher
MATCH (article:Article)
RETURN article.id,
       article.title,
       bm25(article.title, $query) AS score
ORDER BY score ASC
LIMIT 10;
```

For example, the Python API accepts the parameter value through
`Connection.execute`:

```python
statement = """
MATCH (article:Article)
RETURN article.id,
       article.title,
       bm25(article.title, $query) AS score
ORDER BY score ASC
LIMIT 10;
"""

result = connection.execute(
    statement,
    parameters={"query": "graph database"},
)
```

The parameter is bound separately for every execution. It must be present,
have type `STRING`, and not be `NULL`. Its value uses the same full-text query
syntax as a string literal; an empty or syntactically invalid query returns a
query execution error.

## Filtering and Hybrid Search

NeuG applies scalar or graph filters before selecting the final Top-K matches.
This preserves correct Top-K behavior among the eligible nodes.

### Scalar Filtering

Add a `WHERE` predicate to restrict the candidates searched by the FTS index:

```cypher
MATCH (article:Article)
WHERE article.category = 'database'
RETURN article.id,
       article.title,
       bm25(article.title, 'index') AS score
ORDER BY score ASC
LIMIT 10;
```

### Full-Text Search Followed by Graph Traversal

Retrieve the most relevant nodes first, then continue traversing the graph:

```cypher
MATCH (article:Article)
WITH article, bm25(article.title, 'graph database') AS score
ORDER BY score ASC
LIMIT 10
MATCH (article)-[:CITES]->(cited:Article)
RETURN article.title, cited.title, score
ORDER BY score ASC;
```

### Graph Filtering Followed by Full-Text Search

An existing graph pattern can also provide the candidates for FTS ranking:

```cypher
MATCH (author:Author {name: 'Ada'})-[:WROTE]->(article:Article)
RETURN article.id,
       article.title,
       bm25(article.title, 'database') AS score
ORDER BY score ASC
LIMIT 10;
```

## Index Maintenance and Persistence

The FTS index follows changes to its indexed property:

```cypher
// The new article is searchable immediately.
CREATE (:Article {
    id: 1,
    title: 'Graph database indexing',
    category: 'database'
});

// The old text is removed and the new text is indexed.
MATCH (article:Article)
WHERE article.id = 1
SET article.title = 'Full-text retrieval';

// Deleting the node removes it from search results.
MATCH (article:Article)
WHERE article.id = 1
DELETE article;
```

When `SET` assigns `NULL` to an indexed property, the `NULL` value is not
inserted into the FTS index. If the property previously contained indexed
text, its existing index entry is removed.

FTS index data participates in NeuG checkpoints. After reopening a database,
load the `fts` extension before querying the restored index:

```cypher
LOAD fts;

MATCH (article:Article)
RETURN article.id,
       bm25(article.title, 'retrieval') AS score
ORDER BY score ASC
LIMIT 10;
```

## Current Limitations

- An FTS index can be created only on a node property of type `STRING`.
- Each FTS index covers one property; multi-property FTS indexes are not
  supported.
- The `bm25` query argument must be a non-null `STRING` literal or dynamic
  parameter. Other computed expressions are not supported currently.
- `bm25` must be used in a full-text query supported by an FTS index; it is not
  available as a general-purpose scalar function.
- A query can contain only one `bm25` expression.
- A matching FTS index must exist for the property passed to `bm25`.
- If multiple FTS indexes exist on the same property, the query is ambiguous
  and returns an error.
