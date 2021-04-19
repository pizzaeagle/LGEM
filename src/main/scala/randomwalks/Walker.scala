package mm.graph.embeddings
package randomwalks

import graph.{Graph, Relation}

import mm.graph.embeddings.node2vec.{CalculateAliases, CollectNodesNeighbourhood, CreateTripletsWithNeighbourhood, RandomWalk}
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{Dataset, SparkSession}

object Walker {
  def walk(g: Graph, walkLength: Int, numWalks: Int, p: Int = 1, q: Int = 1)
          (implicit spark: SparkSession): Dataset[RandomWalk] = {
    import spark.implicits._
    val nodesWithNeighbours = CollectNodesNeighbourhood(g.edges)
    val tripletWithNeighbourhoods = CreateTripletsWithNeighbourhood(nodesWithNeighbours, g.edges)
    val aliasedTriplets = CalculateAliases(p, q)(tripletWithNeighbourhoods)
    val walkStart = RandomWalk.firstStep(numWalks)(nodesWithNeighbours)
    val walks = RandomWalk.randomWalks(walkLength)(walkStart, aliasedTriplets)
    val cols = Range(1, walkLength + 1).map(x => s"walkStep$x")
    walks.withColumn("walk", array(cols.map(col(_).cast(LongType)):_*))
      .drop(cols:_*)
      .as[RandomWalk]
  }
}
