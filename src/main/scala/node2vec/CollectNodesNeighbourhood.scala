package mm.graph.embeddings
package node2vec

import mm.graph.embeddings.graph.{Node, Relation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

case class Neighbour(id: Long, weight: Double)

case class CollectNodesNeighbourhood(nodeId: Long, neighbourhood: Array[Neighbour])


object CollectNodesNeighbourhood {
  def apply(relations: Dataset[Relation], degree: Option[Int] = None)(implicit spark: SparkSession): Dataset[CollectNodesNeighbourhood] = {
    import spark.implicits._
    degree match {
      case Some(d) => relations
        .distinct
        .groupBy(col("srcID"))
        .agg(slice(sort_array(collect_list(struct(col("weight"), col("dstID").as("id"))).as("neighbourhood")), 1, d).as("neighbourhood")) // todo check slices ranges
        .select(
          col("srcID").as("nodeId"),
          col("neighbourhood")
        )
        .as[CollectNodesNeighbourhood]
      case None => relations
        .distinct
        .groupBy(col("srcID"))
        .agg(collect_list(struct(col("weight"), col("dstID").as("id"))).as("neighbourhood").as("neighbourhood"))
        .select(
          col("srcID").as("nodeId"),
          col("neighbourhood")
        )
        .as[CollectNodesNeighbourhood]
    }
  }
}
