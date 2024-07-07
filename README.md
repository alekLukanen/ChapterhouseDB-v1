# chapterhouseDB
A self-managed streaming data warehouse built on Parquet and DuckDB


## Benchmarks

Install the `benchstat` tool with the following command
```bash
go install golang.org/x/perf/cmd/benchstat@latest
```

You can run the benchmarks with the following command
```bash
go test -v -bench=. -cpu=1 -benchtime=10x -count=10 -benchmem ./...
```

And to write the result to  file with this command and then view the 
results with `benchstat` to view the data in a nicer format
```bash
go test -v -bench=. -cpu=1 -benchtime=10x -count=10 -benchmem ./... > benchmarkResults/proposed.txt
benchstat benchmarkResults/initial.txt benchmarkResults/proposed.txt
```
```
```
