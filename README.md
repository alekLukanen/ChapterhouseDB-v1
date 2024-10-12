# ChapterhouseDB
An extensible data ingestion warehouse build on Apache Arrow and Apache Parquet.
Instead of relying on external data ingestion systems you implement the data warehouse that
meets the needs of your specific use case. This package provides you with a set
of primitive patterns that allow you to build a data warehouse using Golang. The focus of this package
is on the implementation of the data ingestion process. You define tables with a set 
of columns and how those tables are partitioned. Once you insert data into your warehouse
this package manages the distribution of data across a set of works connected to a 
common Redis compatible key-value database and an S3 compatible object storage. Data is 
processed in parallel across your workers allowing you to scale up workers as needed. 
Only one worker can process a table partition at any given time, but if you increase
the number of partitions to a sufficiently large number then it is unlikely you 
will ever limit your ability to process data in parallel. Tables should be partitioned using 
a unique identifier since the partition key also ensures uniqueness of rows in the table. 
Currently integer range and string hash partitioning is available. 
You can have at most 2^32-1 partitions.

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

