# ChapterhouseDB
An extensible data ingestion package build on Apache Arrow and Apache Parquet.
Instead of relying on external data ingestion systems you implement the data system that
meets the needs of your specific use case. This package provides you with a set
of patterns that allow you to build a data warehouse using Golang. The focus of this package
is on the implementation of data ingestion into your data lake. For querying
you can use any query engine of your choice.

An example application and be found [here](https://github.com/alekLukanen/ChapterhouseDB-example-app)  

## View KeyDB

Use the redis-commander tool
```
redis-commander
```

## Querying Local Files With DuckDB

```sql
create secret locals3mock3 (
  TYPE S3,
  KEY_ID "key",
  SECRET "secret",
  ENDPOINT "localhost:9090",
  URL_STYLE "path",
  USE_SSL false
);

select * from 's3://default/chdb/table-state/part-data/table1/0/d_2_0.parquet';
```

