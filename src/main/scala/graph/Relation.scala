package mm.graph.embeddings
package graph

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, when}

case class Relation(srcID: Long, dstID: Long, weight: Option[Double] = None)


object Relation {
  def readUndirected[T](data: T)(implicit spark: SparkSession): Dataset[Relation] = {
    import spark.implicits._
    val relDF = readDirected(data)
    val invertedRelDF = relDF.select(
      col("srcID").as("dstID"),
      col("dstID").as("srcID"),
      col("weight")
    ).as[Relation]
    relDF.union(invertedRelDF)
  }

  def readDirected[T](data: T)(implicit spark: SparkSession): Dataset[Relation] = {
    import spark.implicits._
    val df = data match {
      case path: String => spark.read.csv(path).as[Relation]
      case relations: Seq[Relation] => relations.toDS
      case _ => throw new Exception("Unsupported relation data type")
    }
    df.withColumn("weight", when(col("weight").isNull, 1.0).otherwise(col("weight")))
      .as[Relation]
  }
}

