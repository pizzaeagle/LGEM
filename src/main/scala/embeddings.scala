package mm.graph

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

package object embeddings {
  implicit val spark: SparkSession = {
    SparkSession
      .builder()
      .getOrCreate()
  }
  implicit val config: Configuration = Configuration(ConfigFactory.load())

}
