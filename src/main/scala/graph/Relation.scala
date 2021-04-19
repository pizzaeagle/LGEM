package mm.graph.embeddings
package graph

import com.sun.tools.javac.code.TypeTag
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, when}

case class Relation(srcID: Long, dstID: Long, weight: Option[Double] = None)


object Relation {
  def readUndirected[T](data: T)(implicit spark: SparkSession): Dataset[Relation] = {
    import spark.implicits._
    val relDF = readDirected(data)
    val invertedRelDF = relDF
      .withColumnRenamed("srcID", "dstId_new")
      .withColumnRenamed("dstID", "srcID")
      .withColumnRenamed("dstId_new", "dstID")
      .drop("dstId_new")
      .as[Relation]
    relDF.union(invertedRelDF)
  }

  def readDirected[T](data: T)(implicit spark: SparkSession): Dataset[Relation] = {
    import spark.implicits._
    val df = data match {
      case path: String => spark.read.csv(path)
      case relations: Seq[Relation] => relations.toDS
      case _ => throw new Exception("Unsupported relation data type")
    }
    df.withColumn("weight", when(col("weight").isNull, 1.0).otherwise(col("weight")))
      .as[Relation]
  }
}

