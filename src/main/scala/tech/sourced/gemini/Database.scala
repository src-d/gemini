package tech.sourced.gemini

import com.datastax.driver.core.{Row, Session}
import com.datastax.driver.core.querybuilder.QueryBuilder

import scala.collection.JavaConverters._

case class MetaCols(sha: String, repo: String, commit: String, path: String)
case class HashtablesCols(sha: String, hashtable: String, value: String)
case class FeaturesDocsCols(id: String, docs: String)
case class FeaturesFreqCols(id: String, feature: String, weight: String)

/**
  * Tables is static typed definition of DB schema
  *
  * @param meta name of meta table
  * @param hashtables prefix of hashtables table
  * @param featuresDocs name of features documents table
  * @param featuresFreq name of features frequencies table
  * @param metaCols columns of meta table
  * @param hashtablesCols columns of hashtables table
  * @param featuresDocsCols columns of features documents table
  * @param featuresFreqCols columns of features frequencies table
  */
case class Tables(meta: String,
                  hashtables: String,
                  featuresDocs: String,
                  featuresFreq: String,
                  metaCols: MetaCols,
                  hashtablesCols: HashtablesCols,
                  featuresDocsCols: FeaturesDocsCols,
                  featuresFreqCols: FeaturesFreqCols) {
  def hashtables(mode: String): String = s"${hashtables}_$mode"
}

/**
  * Database object contains common queries to DB
  */
object Database {
  /**
    * query database by hash and return files with that hash
    *
    * @param sha
    * @param conn
    * @param keyspace
    * @param tables
    * @return
    */
  def findFilesByHash(sha: String, conn: Session, keyspace: String, tables: Tables): Iterable[RepoFile] = {
    val query = QueryBuilder.select().all().from(keyspace, tables.meta)
      .where(QueryBuilder.eq(tables.metaCols.sha, sha))

    conn.execute(query).asScala.map(rowToRepoFile(tables))
  }

  private def rowToRepoFile(tables: Tables)(row: Row): RepoFile = {
    val cols = tables.metaCols
    RepoFile(row.getString(cols.repo), row.getString(cols.commit), row.getString(cols.path), row.getString(cols.sha))
  }
}
