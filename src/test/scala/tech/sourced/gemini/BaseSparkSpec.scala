package tech.sourced.gemini

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait BaseSparkSpec extends BeforeAndAfterAll {
  this: Suite =>

  @transient var sparkSession: SparkSession = _
  private var _conf: SparkConf = _

  def useSparkConf(conf: SparkConf): SparkConf = {
    _conf = conf
    _conf
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .config(_conf)
      .config("spark.driver.host", "localhost")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    resetSparkContext()
  }

  def resetSparkContext(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
    sparkSession = null
  }
}
