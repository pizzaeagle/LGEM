package mm.graph

import org.apache.spark.sql.SparkSession

package object embeddings {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
}
