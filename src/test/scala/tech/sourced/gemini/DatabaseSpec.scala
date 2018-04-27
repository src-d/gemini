package tech.sourced.gemini

import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import tags.DB

@tags.DB
class DatabaseSpec extends FlatSpec
  with Matchers
  with BaseDBSpec
  with Logging
  with BeforeAndAfterAll {

  keyspace = "databaseSpec"

  override def beforeAll(): Unit = {
    super.beforeAll()

    insertMeta(Array(
      RepoFile("repo1", "commit1", "path1", "c4e5bcc8001f80acc238877174130845c5c39aa3"),
      RepoFile("repo1", "commit2", "path2", "c4e5bcc8001f80acc238877174130845c5c39aa3"),
      RepoFile("repo2", "commit1", "path1", "another-cache")
    ))
  }

  "Query for duplicates" should "return 2 files" in {
    val sha1 = Database.repoFilesByHash("c4e5bcc8001f80acc238877174130845c5c39aa3", cassandra, keyspace, Gemini.tables)

    sha1 should not be empty
    sha1.size shouldEqual 2
  }
}
