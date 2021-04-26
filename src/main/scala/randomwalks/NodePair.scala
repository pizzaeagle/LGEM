package mm.graph.embeddings
package randomwalks

import graph.{Graph, Node}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random

case class NodePair(node1: Long, node2: Long, linked: Boolean)
case class Relation(srcID: Long, dstID: Long, weight: Option[Double] = None)

object NodePair {

  def getPositivePairs(g: Graph, numSamples: Int)(implicit spark: SparkSession): Dataset[NodePair] = {
    import spark.implicits._
    val w = Window.partitionBy("srcID").orderBy(rand())
    val positive = g.edges
      .drop("weight")
      .withColumn("row", row_number().over(w))
      .where(col("row") <= numSamples)
      .withColumnRenamed("srcID", "node1")
      .withColumnRenamed("dstID", "node2")
      .drop("row")
      .withColumn("linked", lit(true))
      .as[NodePair]

//    val negative = g.edges.crossJoin(g.vertices.as("v"))
//      .where(col("dstID") =!= col("nodeId"))
//      .withColumn("row", row_number().over(w))
//      .where(col("row") <= numSamples)
//      .select(col("srcID").as("node1"),
//        col("dstID").as("node2"))
//      .withColumn("linked", lit(false))
//      .as[NodePair]
//    positive.union(negative)
    positive
  }

  def getNegativePairs(g: Graph, numSamples: Int)(implicit spark: SparkSession): Dataset[NodePair] = {
    import spark.implicits._
    g.vertices
      .mapPartitions(partition => {
        val list = partition.toIndexedSeq
        list.map { row =>
          val Node(_, nodeId, _) = row
          val r = Range(0, numSamples).map(_ => list(Random.nextInt(list.length)).nodeId)
          Pairs(nodeId, r)
        }
      }.toIterator)
      .withColumn("node2", explode(col("negativePairs")))
      .withColumn("linked", lit(false))
      .drop("negativePairs")
      .as[NodePair]
  }
}

case class Pairs(node1: Long, negativePairs: Seq[Long])
