# ChapterhouseDB
A self-managed streaming data warehouse built on Parquet with an interchangeable
query engine to support single node query engines like DuckDB and DataFusion.

## Benchmarks

Install the `benchstat` tool with the following command
```bash
go install golang.org/x/perf/cmd/benchstat@latest
```

You can run the benchmarks with the following command
```bash
go run cmd/benchmarks/main.go -new
```
This will generate a new benchmark file in the `benchmarkResults/` directory
And compare against the previous benchmark file.

Or you can run the benchmarks with the following commands
```bash
go test -v -bench=. -cpu=1 -benchtime=10x -count=10 -benchmem ./...
```

And to write the result to  file with this command and then view the 
results with `benchstat` to view the data in a nicer format
```bash
go test -v -bench=. -cpu=1 -benchtime=10x -count=10 -benchmem ./... > benchmarkResults/proposed.txt
benchstat benchmarkResults/initial.txt benchmarkResults/proposed.txt
```

## Potential Issues

* Need to add an `_event_ts` on each row to support ignoring late data. The
event will still be written to history but not to the main state table.
