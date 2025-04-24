# TinySNB dataset

This dataset is originated from [kuzu](https://github.com/kuzudb/kuzu/tree/master/dataset/tinysnb), with some minor changes.

- Currently we only constructed `person` and `konws`
- The `union`, `struct` type are not supported now
- The `day` and `date` type are currently not fully supported(parsing from the raw csvs will fail, maybe some format are not supported. To do in the future).