# gemini go
> Find similar code in Git repositories

Trivial example of the client application in Golang to query Scylla/Cassandra.

*Disclamer*: _this is experimental, only a query for *duplicate files* is supported_.


# Run
Add path to the file you want to search duplicate files for and

```
go run src/main/go/query.go <path-to-file>
```

or, to test a connection to DB use

```
go run -tags="gocql_debug" src/main/go/connect.go
```
