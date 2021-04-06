package mm.graph

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

package object embeddings {
  implicit val spark: SparkSession = {
    val s = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    LogManager.getRootLogger.setLevel(Level.WARN)
    s
  }
}
