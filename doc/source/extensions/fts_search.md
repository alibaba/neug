# Full-Text Search Extension

Since NeuG **v0.2.0**, the `fts` extension provides full-text indexes and
BM25-ranked search over node string properties, with automatic index
maintenance and persistence.

For syntax and guarantees shared by all index types, including inspection,
transactions, and recovery, see [Storage Indexes](../storage_index/index.md).

The FTS extension supports:

- Full-text indexes on one or more node properties of type `STRING`
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

An FTS index can be created on one or more `STRING` properties of a node table
by specializing the common
[CREATE INDEX](../storage_index/index.md#create-an-index) syntax:

```cypher
CREATE INDEX <index_name> [IF NOT EXISTS]
ON <node_table>
USING FTS (<string_property> [, <string_property> ...])
[WITH (
    tokenizer = '<tokenizer>',
    jieba_mode = '<jieba_mode>',
    jieba_dict = '<dictionary_path>',
    prefix = '<prefix_lengths>'
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

To inspect or remove an FTS index, use the common
[SHOW_INDEXES](../storage_index/index.md#inspect-indexes) and [DROP
INDEX](../storage_index/index.md#drop-an-index) operations.

Dropping a property that belongs to an FTS index removes the entire index. For
example, dropping `title` from an index on `(title, content)` also removes the
index on `content`; recreate the index with the remaining properties if it is
still needed.

### Create an FTS Index on Multiple Properties

A single FTS index can cover multiple `STRING` properties. This is useful for
documents whose searchable text is split across fields such as `title` and
`content`. The index keeps the inverted data for all listed properties in one
index structure. At query time, you can search any indexed property or a
selected group of indexed properties, and assign a different BM25 weight to
each selected property. See [Multi-Property Search](#multi-property-search).

List the properties in the `USING FTS` clause:

```cypher
CREATE NODE TABLE Article (
    id INT64 PRIMARY KEY,
    title STRING,
    category STRING,
    content STRING
);

CREATE INDEX article_text_fts
ON Article
USING FTS (title, content);
```

Here, `(title, content)` specifies the two properties maintained by
`article_text_fts`. Every indexed property must belong to the same node table
and have type `STRING`.

### Index Options

The `WITH` clause accepts the following case-sensitive option names:

| Option | Description | Default |
| --- | --- | --- |
| `tokenizer` | Tokenization strategy used to split indexed text into searchable terms | `unicode61` |
| `jieba_mode` | Jieba algorithm: `mp`, `hmm`, or `mix`; valid only when `tokenizer = 'jieba'` | `mix` |
| `jieba_dict` | Path to a Jieba user dictionary that supplements the built-in dictionary; valid only when `tokenizer = 'jieba'` | No user dictionary |
| `prefix` | Space-separated token lengths for prefix indexes, such as `2 3` | No prefix index |

### Tokenizers

Supported tokenizers are:

- `unicode61` splits text according to Unicode 6.1 rules. This is the default.
- `ascii` applies ASCII tokenization rules. Characters outside the ASCII range
  (0-127) are treated as token characters.
- `porter` applies the Porter stemming algorithm so related word forms can
  match the same stem.
- `trigram` treats each contiguous sequence of three characters as a token,
  enabling substring matching.
- `jieba` performs Chinese word segmentation using cppjieba and loads the
  built-in small dictionary and HMM model.

The Jieba tokenizer supports three modes:

- `mp` selects the most probable dictionary-based segmentation.
- `hmm` uses the HMM model to recognize words without dictionary guidance.
- `mix` combines dictionary segmentation with HMM recognition. This is the
  default Jieba mode.

Chinese word segmentation uses a dictionary to recognize words and produce
more accurate semantic boundaries. A dictionary must be a text file whose
contents are UTF-8 encoded, including when the file is read in binary mode. Its
entries use the following format:

```text
word frequency part-of-speech
```

NeuG includes a small dictionary containing about 110,000 entries, generated
from cppjieba's `test/testdata/extra_dict/jieba.dict.small.utf8`. The smaller
dictionary may omit common or domain-specific terms and reduce search recall.
To improve segmentation, configure `jieba_dict` with additional terms, such as
those from cppjieba's full dictionary at `dict/jieba.dict.utf8` (about 350,000
entries) or from another compatible dictionary.

For example:

```cypher
CREATE INDEX article_title_fts
ON Article
USING FTS (title)
WITH (
    tokenizer = 'jieba',
    jieba_mode = 'mix',
    jieba_dict = '/path/to/user.dict.utf8'
);
```

The dictionary specified by `jieba_dict` is added to the built-in small
dictionary; it supplements rather than replaces the built-in entries. Following
cppjieba's user-dictionary format, each line may contain only a word or may also
include its frequency and part of speech. The dictionary path must remain
available when the index is reopened. Relative paths are resolved against the
process working directory when the index is created and persisted as absolute
paths.

For more details about dictionary formats and usage, see the
[cppjieba README](https://github.com/yanyiwu/cppjieba/blob/master/README.md) and
the [jieba README](https://github.com/fxsjy/jieba/blob/master/README.md).

For example, the Porter tokenizer can be combined with `unicode61`, and prefix
indexes can be created for two- and three-character prefixes:

```cypher
CREATE INDEX article_title_fts
ON Article
USING FTS (title)
WITH (
    tokenizer = 'porter unicode61',
    prefix = '2 3'
);
```

Invalid tokenizer, tokenizer parameters (for example, `jieba_mode`), prefix,
or unsupported settings cause index creation to fail.
The selected settings cannot be changed in place; drop and recreate the index
to use different settings.

## Full-Text Search

NeuG provides two forms of `bm25`:

- `bm25(indexed_property, query)` searches one indexed property.
- `bm25([property1, property2, ...], [weight1, weight2, ...], query)`
  searches multiple indexed properties with per-property weights.

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

### Multi-Property Search

For an FTS index containing multiple properties, pass a property list and a
corresponding weight list to `bm25`:

```cypher
MATCH (article:Article)
RETURN article.id,
       article.title,
       bm25(
           [article.title, article.content],
           [5.0, 1.0],
           'graph database'
       ) AS score
ORDER BY score ASC
LIMIT 10;
```

Arguments:

- **Properties**: The first list selects the indexed properties to search.
- **Weights**: The second argument assigns a weight to each property by
  position. A larger weight gives that property more influence on the BM25
  ranking. In addition to a list, this argument also accepts an array.
- **Query**: The final argument is the full-text query described in
  [Query Syntax](#query-syntax).

In the example, `title` has weight `5.0` and `content` has weight `1.0`.

Search behavior:

- A multi-property index can search one indexed property or any combination of
  its indexed properties.
- Only properties passed to `bm25` participate in matching and ranking.
- Values from indexed properties not passed to `bm25` are ignored.
- Use `bm25(property, query)` to search one property.
- Use `bm25([properties], weights, query)` to search a property combination.

Requirements:

- All selected properties must belong to the same node variable and the same
  FTS index.
- The property list and weight list must be non-empty and have the same length.
- Each weight must be numeric, positive, finite, and not `NULL`.
- Weights can be literals or dynamic parameters; see
  [Dynamic Query Parameters](#dynamic-query-parameters).
- For an FTS index created on one property, use
  `bm25(indexed_property, query)`. The multi-property form
  `bm25([properties], weights, query)` is only supported for an index created
  on multiple properties.

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

For multi-property search, the weight list can also be supplied as a dynamic
parameter:

```cypher
MATCH (article:Article)
RETURN article.id,
       bm25([article.title, article.content], $weights, $query) AS score
ORDER BY score ASC
LIMIT 10;
```

```python
statement = """
MATCH (article:Article)
RETURN article.id,
       bm25([article.title, article.content], $weights, $query) AS score
ORDER BY score ASC
LIMIT 10;
"""

result = connection.execute(
    statement,
    parameters={
        "weights": [5.0, 1.0],
        "query": "graph database",
    },
)
```

For `$weights`:

- The value can be supplied as a list. An array is also accepted.
- It must contain one value for each property, in the same order.
- Every value must be numeric, positive, finite, and not `NULL`.
- `$weights` and `$query` can be bound independently or used together.

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

## FTS Index Maintenance

Inserts, updates, and deletes participate in the common [transactional index
maintenance](../storage_index/index.md#transactions). For example:

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

Persistent properties do not currently support `NULL`. Assigning `NULL` to an
indexed property fails without changing the property or its index entry.

For checkpoint and reopen behavior, including loading `fts` to activate a
restored index, see [Persistence and Recovery](../storage_index/index.md#persistence-and-recovery).

## Current Limitations

- An FTS index can be created only on one or more node properties of type
  `STRING`.
- The `bm25` query argument must be a non-null `STRING` literal or dynamic
  parameter. Other computed expressions are not supported currently.
- `bm25` must be used in a full-text query supported by an FTS index; it is not
  available as a general-purpose scalar function.
- A query can contain only one `bm25` expression.
- A matching FTS index must exist for the property passed to `bm25`.
- If multiple FTS indexes exist on the same property, the query is ambiguous
  and returns an error.
