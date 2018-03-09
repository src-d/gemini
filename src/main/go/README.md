# gemini
> Find similar code in Git repositories

Trivial example of client application in Golang to query for same files.


# Run
Add path to the file you want to search duplicates for and

```
go run src/main/go/query.go <path-to-file>
```

or, to test a connection to DB use

```
go run -tags="gocql_debug" src/main/go/connect.go
```
