package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"gopkg.in/src-d/go-git.v4/plumbing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

type BlobHash struct {
	BlobHash string
	Repo     string
	FilePath string
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) <= 0 {
		fmt.Printf("Missing mandatory <filename>\n")
		os.Exit(2)
	}

	file := flag.Arg(0)
	hash := sha1hash(file)
	fmt.Printf("Looking for %s\n", hash)

	session := connect()
	defer session.Close()

	stmt, names := qb.Select("hashes.blob_hash_files").
		Where(qb.In("blob_hash")).
		ToCql()

	q := gocqlx.Query(session.Query(stmt), names).BindMap(qb.M{
		"blob_hash": []string{hash},
	})
	defer q.Release()

	var similarHashes []BlobHash
	if err := gocqlx.Select(&similarHashes, q.Query); err != nil {
		log.Fatal("select:", err)
	}

	fmt.Printf("%+v\n", similarHashes)
}

// connect to the cluster
func connect() *gocql.Session {
	node := "127.0.0.1"
	cluster := gocql.NewCluster(node)
	cluster.Keyspace = "hashes"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Can not create connection to %s, %v", node, err)
	}
	return session
}

func sha1hash(file string) string {
	fd, err := os.Stat(file)
	fmt.Printf("Reading file:%s size:%d\n", file, fd.Size())
	if os.IsNotExist(err) {
		log.Fatalf("Path %s does not exist", file)
	}

	f, err := os.Open(file)
	if err != nil {
		log.Fatal("Can not open a file %s", file, err)
	}
	defer f.Close()

	h := plumbing.NewHasher(plumbing.BlobObject, fd.Size())
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	return h.Sum().String()
}
