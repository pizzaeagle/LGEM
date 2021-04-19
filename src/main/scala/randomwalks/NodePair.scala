package mm.graph.embeddings
package randomwalks

import graph.{Graph, Node}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random

case class NodePair(node1: Long, node2: Long, linked: Boolean)

object NodePair {

  def getPositivePairs(g: Graph, walkLength: Int, numWalks: Int)
                    (implicit spark: SparkSession): Dataset[NodePair] = {
    import spark.implicits._
    Walker.walk(g, walkLength, numWalks)
      .withColumn("node2", element_at(col("walk"), -1))
      .withColumnRenamed("srcID", "node1")
      .withColumn("linked", lit(1))
      .drop("walkNumber", "walk")
      .as[NodePair]

  }

  def getNegativePairs(g: Graph, numSamples: Int)
                      (implicit spark: SparkSession): Dataset[NodePair] = {
    import spark.implicits._
    g.vertices.repartition(1).mapPartitions(partition => {
      val list = partition.toIndexedSeq
      list.map { row =>
        val Node(_, nodeId, _) = row
        val r = Range(0, numSamples).map(_ => list(Random.nextInt(list.length)).nodeId)
        Pairs(nodeId, r)
      }
    }.toIterator)
      .withColumn("node2", explode(col("negativePairs")))
      .withColumn("linked", lit(0))
      .drop("negativePairs")
      .as[NodePair]
  }
}

case class Pairs(node1: Long, negativePairs: Seq[Long])
