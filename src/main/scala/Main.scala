package mm.graph.embeddings

import mm.graph.embeddings.graph.{IndexedNode, Relation}
import mm.graph.embeddings.node2vec.{NodesNeighbourhood, RandomWalk, TripletWithAlias, TripletWithNeighbourhoods}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object Main {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val degree = Some(3)
    val p = 1
    val q = 1
    val walkLength = 10
    val numWalks = 10

    val relDF = Relation.readUndirected(TestData.relations)
//    val nodeDF = IndexedNode.read(TestData.nodes)
    val nodesWithNeighbours = NodesNeighbourhood(relDF, degree)
    val tripletWithNeighbourhoods = TripletWithNeighbourhoods(nodesWithNeighbours, relDF)
    val aliasedTriplets = TripletWithAlias(p, q)(tripletWithNeighbourhoods)
    aliasedTriplets.show(100,false)
    val walkStart = RandomWalk.firstStep(numWalks)(nodesWithNeighbours)
    val x = RandomWalk.randomWalks(walkLength)(walkStart, aliasedTriplets)

    val cols = Range(1, 11).map(x => col(s"walkStep$x").cast(StringType))
    val rwalks = x.withColumn("randomWalk", array(cols:_*))
    rwalks.show(false)
    println(rwalks.count)


    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("randomWalk")
      .setOutputCol("result")
      .setVectorSize(10)
      .setMinCount(0)
    val model = word2Vec.fit(rwalks)

    model.getVectors.show(100,false)

  }
}
