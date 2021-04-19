package mm.graph.embeddings
package node2vec

import graph.Relation

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}


case class CreateTripletsWithNeighbourhood(srcID: Long,
                                           dstID: Long,
                                           weight: Option[Double] = None,
                                           srcNeighbourhood: Array[Neighbour],
                                           dstNeighbourhood: Array[Neighbour])

object CreateTripletsWithNeighbourhood {
  def apply(nodes: Dataset[CollectNodesNeighbourhood], rel: Dataset[Relation])
           (implicit spark: SparkSession): Dataset[CreateTripletsWithNeighbourhood] = {
    import spark.implicits._
    nodes.join(rel, nodes("nodeId") === rel("srcID"))
      .select(
        col("srcID"),
        col("dstID"),
        col("weight"),
        col("neighbourhood").as("srcNeighbourhood")
      )
      .join(nodes, nodes("nodeId") === rel("dstID"))
      .select(
        col("srcID"),
        col("dstID"),
        col("weight"),
        col("srcNeighbourhood"),
        col("neighbourhood").as("dstNeighbourhood")
      ).distinct.as[CreateTripletsWithNeighbourhood]
  }
}


