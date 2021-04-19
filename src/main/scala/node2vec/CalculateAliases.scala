package mm.graph.embeddings
package node2vec

import mm.graph.embeddings.node2vec.Alias.setupAliasUdf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

case class CalculateAliases(srcID: Long,
                            dstID: Long,
                            srcNeighbourhood: Array[Long],
                            dstNeighbourhood: Array[Long],
                            j: Array[Int],
                            q: Array[Double])


object CalculateAliases{
  def apply(p: Double = 1.0, q: Double = 1.0)(df: Dataset[CreateTripletsWithNeighbourhood])
           (implicit spark: SparkSession): Dataset[CalculateAliases] = {
    import spark.implicits._
    df
      .withColumn("dstNeighbour", explode(col("dstNeighbourhood")))
      .withColumn("unormProb", when(col("dstNeighbour.id") === col("srcId"), col("dstNeighbour.weight") / p)
        .otherwise(when(array_contains(col("srcNeighbourhood.id"),col("dstNeighbour.id")), col("dstNeighbour.weight"))
          .otherwise(col("dstNeighbour.weight") / q))
      )
      .groupBy("srcId", "dstId", "srcNeighbourhood")
      .agg(collect_list(col("dstNeighbour.id")).as("dstNeighbourhood"),
        collect_list(col("unormProb")).as("unormProbs"))
      .select(
        col("srcID"),
        col("dstID"),
        col("srcNeighbourhood"),
        col("dstNeighbourhood"),
        col("unormProbs")
      )
      .withColumn("aliases", setupAliasUdf(col("unormProbs")))
      .select(
        col("srcID"),
        col("dstID"),
        col("srcNeighbourhood.id").as("srcNeighbourhood"),
        col("dstNeighbourhood"),
        col("aliases.j").as("j"),
        col("aliases.q").as("q")
      ).as[CalculateAliases]
      .repartition(1)
  }
}