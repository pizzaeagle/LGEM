package mm.graph.embeddings
package graph

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.StructType

case class Relation(srcID: Long, dstID: Long, weight: Option[Double] = None)

object Relation {
  val schema = ScalaReflection.schemaFor[Relation].dataType.asInstanceOf[StructType]

  def readUndirected[T](data: T)(implicit spark: SparkSession): Dataset[Relation] = {
    import spark.implicits._
    val relDF = readDirected(data)
    val invertedRelDF = relDF
      .select(col("srcID").as("dstID"), col("dstID").as("srcID"), col("weight"))
      .select(
        "srcID",
        "dstID",
        "weight"
      )
      .as[Relation]
    relDF.union(invertedRelDF)
  }

  def readDirected[T](data: T)(implicit spark: SparkSession): Dataset[Relation] = {
    import spark.implicits._
    val df = data match {
      case path: String =>
        spark.read
          .option("header", "true")
          .csv(path)
          .select(col("srcID").cast("long"), col("dstID").cast("long"), lit(1.0).as("weight"))
      case relations: Seq[Relation] => relations.toDS
      case _                        => throw new Exception("Unsupported relation data type")
    }
    df.withColumn("weight", when(col("weight").isNull, 1.0).otherwise(col("weight")))
      .as[Relation]
  }
}
