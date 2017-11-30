package tech.sourced.gemini

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait BaseSparkSpec extends BeforeAndAfterAll
  /*with BeforeAndAfterEach*/ {
  this: Suite =>

  @transient var ss: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    ss = SparkSession.builder()
      .appName("test").master("local[*]")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    resetSparkContext()
  }

  def resetSparkContext(): Unit = {
    if (ss != null) {
      ss.stop()
    }
    ss = null
  }
}