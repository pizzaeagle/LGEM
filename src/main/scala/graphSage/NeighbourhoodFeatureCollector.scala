package mm.graph.embeddings
package graphSage

import graph.Graph

import mm.graph.embeddings.randomwalks.NodePair
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{array_except, array_intersect, array_union, col, collect_list, collect_set, explode, flatten, lit, slice, udf}

import java.sql.Timestamp
import scala.util.Try

object NeighbourhoodFeatureCollector {
  def collectNeighbourhood(g: Graph): DataFrame = {
    g.edges.as("g1")
      .join(g.edges.as("g2"), col("g1.dstID") === col("g2.srcID")
        and col("g1.srcID") =!= col("g2.dstID"))
      .select(
        col("g1.srcID").as("n1"),
        col("g1.dstID").as("n2"),
        col("g2.dstID").as("n3")
      )
      .groupBy("n1")
      .agg(
        collect_set(col("n2")).as("n2"),
        collect_set(col("n3")).as("n3")
      )
      .withColumn("n3", array_except(col("n3"), array_intersect(col("n2"), col("n3"))))
  }

  def collectNeighbourhoodFeatures(df: DataFrame, g: Graph, firstFeaturesVectorLength: Int, secondFeaturesVectorLength: Int): DataFrame = {
    df
      .withColumn("n2", explode(col("n2")))
      .withColumn("n3", explode(col("n3")))
      .join(g.vertices, col("n1") === col("nodeId"))
      .withColumnRenamed("features", "features1")
      .drop("nodeName", "nodeId")
      .join(g.vertices, col("n2") === col("nodeId"))
      .withColumnRenamed("features", "features2")
      .drop("nodeName", "nodeId")
      .join(g.vertices, col("n3") === col("nodeId"))
      .withColumnRenamed("features", "features3")
      .drop("nodeName", "nodeId")
      .groupBy("n1", "features1")
      .agg(
        collect_list(col("features2")).as("features2"),
        collect_list(col("features3"))as("features3")
      )
      .withColumn("features2", append_zeros(col("features2"), lit(firstFeaturesVectorLength)))
      .withColumn("features3", append_zeros(col("features3"), lit(secondFeaturesVectorLength)))
      .withColumn("features2", flatten(slice(col("features2"), 1, firstFeaturesVectorLength)))
      .withColumn("features3", flatten(slice(col("features3"), 1, secondFeaturesVectorLength)))
      .withColumnRenamed("n1", "nodeID")
  }

  def createPairWithFeatures(pairs: Dataset[NodePair], neighbourhoodFeatures: DataFrame): DataFrame = {
    pairs.join(neighbourhoodFeatures, col("node1") === col("nodeID"))
      .withColumnRenamed("features1", "features1_1")
      .withColumnRenamed("features2", "features2_1")
      .withColumnRenamed("features3", "features3_1")
      .drop("nodeID")
      .join(neighbourhoodFeatures, col("node2") === col("nodeID"))
      .withColumnRenamed("features1", "features1_2")
      .withColumnRenamed("features2", "features2_2")
      .withColumnRenamed("features3", "features3_2")
      .drop("nodeID")
  }

  val append_zeros = udf((arr: Seq[Seq[Double]], length: Int) => {
    val zeros = arr.head.map(_ => 0.0)
    Range(0, length - arr.length).foldLeft(arr)((arr, _) => arr :+ zeros)
  })
}
