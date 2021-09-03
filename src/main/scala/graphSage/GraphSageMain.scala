package mm.graph.embeddings
package graphSage

import graph.{Graph, Node, Relation}
import randomwalks.NodePair

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object GraphSageMain {

  val array_divide = udf[Seq[Double], Seq[Long]](
    arr => arr.map(x => x.asInstanceOf[Double] / 4713)
  )

  def run()(implicit spark: SparkSession, configuration: Configuration) = {
    import spark.implicits._

    val relDF = Relation
      .readUndirected(configuration.relationsInputDir)
      .as[Relation]
      .as[Relation]
      .repartition(1)
      .cache
    val nodeDF = spark.read
      .json(configuration.nodeFeaturesInputDir)
      .select(col("nodeName"), col("nodeName").as("nodeID").cast("long"), array_divide(col("features")).as("features"))
      .as[Node]
      .repartition(1)
      .cache

    val g = Graph(nodeDF, relDF)
    val neighbourhood = NeighbourhoodFeatureCollector.collectNeighbourhood(g)
    val pairsDS = NodePair.getPositivePairs(g, 3)
    val featuresDF = NeighbourhoodFeatureCollector.collectNeighbourhoodFeatures(neighbourhood, g, 5, 10).repartition(1)

    val input = NeighbourhoodFeatureCollector.createPairWithFeatures(pairsDS, featuresDF)
    input.write.mode("overwrite").parquet(configuration.outputDir)
  }
}
