package mm.graph.embeddings
package node2vec

import graph.Relation

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.types.StringType

object Node2VecMain {
  def run()(implicit spark: SparkSession, configuration: Configuration): Unit = {

    val degree = Some(3)
    val p = 1
    val q = 1
    val walkLength = 10
    val numWalks = 10

    val relDF =
      Relation.readUndirected(configuration.relationsInputDir)
    //    val nodeDF = IndexedNode.read(TestData.nodes)
    val nodesWithNeighbours = CollectNodesNeighbourhood(relDF, degree)
    val tripletWithNeighbourhoods = CreateTripletsWithNeighbourhood(nodesWithNeighbours, relDF)
    val aliasedTriplets = CalculateAliases(p, q)(tripletWithNeighbourhoods)
    val walkStart = RandomWalk.firstStep(numWalks)(nodesWithNeighbours)
    val x = RandomWalk.randomWalks(walkLength)(walkStart, aliasedTriplets)

    val cols = Range(1, 11).map(x => col(s"walkStep$x").cast(StringType))
    val rwalks = x.withColumn("randomWalk", array(cols: _*))

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("randomWalk")
      .setOutputCol("result")
      .setVectorSize(10)
      .setMinCount(0)
    val model = word2Vec.fit(rwalks)

    model.getVectors.show(100, false)
    model.getVectors.write.mode("overwrite").parquet(configuration.outputDir)
  }
}
