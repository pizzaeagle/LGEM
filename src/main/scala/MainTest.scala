package mm.graph.embeddings

import graph.Graph

import mm.graph.embeddings.TestData.{nodes, relations}
import mm.graph.embeddings.graphSage.NeighbourhoodFeatureCollector
import mm.graph.embeddings.randomwalks.{NodePair, Walker}

object MainTest extends App{
  import spark.implicits._
  val g = TestData.testGraphGeneration(500, 5, 10)
  g.vertices.show(false)
  g.edges.show(false)

  val neighbourhood = NeighbourhoodFeatureCollector.collectNeighbourhood(g)
  val positiveDS = NodePair.getNegativePairs(g,3)
  val negativeDS = NodePair.getPositivePairs(g,3,3)
  val pairs = positiveDS.union(negativeDS)
  val featuresDF = NeighbourhoodFeatureCollector.collectNeighbourhoodFeatures(neighbourhood, g, 5, 10)
  val input = NeighbourhoodFeatureCollector.createPairWithFeatures(pairs, featuresDF).repartition(1)
  input.write.mode("overwrite").parquet("/Users/mmstowski/IdeaProjects/lgem/warehouse/test_input")

  spark.read.parquet("/Users/mmstowski/IdeaProjects/lgem/warehouse/test_input").show(100, false)
}
