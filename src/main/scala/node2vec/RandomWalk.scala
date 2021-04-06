package mm.graph.embeddings
package node2vec

import mm.graph.embeddings.node2vec.Alias.{drawAliasUdf, setupAliasUdf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{element_at, _}

case class RandomWalk(srcId: Long, walkNumber: Int, walk: Seq[Long])


object RandomWalk {
  def firstStep(walkNumber: Int)(dataset: Dataset[NodesNeighbourhood])(implicit spark: SparkSession): Dataset[RandomWalk] = {
    import spark.implicits._
    val df = dataset.withColumn("alias", setupAliasUdf(col("neighbourhood.weight")))
      .withColumnRenamed("nodeId", "srcId")
    val walks = Seq.empty[RandomWalk].toDS

    Range(1, walkNumber + 1).foldLeft(walks){
      (walks, i) => {
        val singleWalk = df
        .withColumn("walkNumber", lit(i))
        .withColumn("walk", array(col("srcId"),col("neighbourhood.id")(drawAliasUdf(col("alias.j"), col("alias.q")))))
        .select(
          col("srcId"),
          col("walkNumber"),
          col("walk")
        )
          .as[RandomWalk]
        walks.union(singleWalk)
      }
    }
  }

  def randomWalks(walkLength: Int)(walkStartDS: Dataset[RandomWalk], relDS: Dataset[TripletWithAlias])
                 (implicit spark: SparkSession): DataFrame /*Dataset[RandomWalk] */ = {
    import spark.implicits._

    val relRDS = relDS
      .withColumnRenamed("srcID", "rel_srcID")
      .withColumnRenamed("dstID", "rel_dstID")

//    Range(1, walkLength - 1).foldLeft(walkStartDS){
//      (walks, i) => {
//        walks.where("srcId == 7 and walkNumber == 7").show(100, false)
//        val x = walks
//          .withColumn("stepStart", element_at(col("walk"), -2))
//          .withColumn("stepEnd", element_at(col("walk"), -1))
//          .join(relRDS, col("stepStart") === col("rel_srcID") and col("stepEnd") === col("rel_dstID"))
//          .withColumn("walk", concat(col("walk"), array(element_at(col("dstNeighbourhood"), drawAliasUdf(col("j"), col("q")) + 1))))
//          .withColumn("x", drawAliasUdf(col("j"), col("q")))
//
//        x.where("srcId == 7 and walkNumber == 7").show(100, false)
//        x
//          .select(
//            col("srcId"),
//            col("walkNumber"),
//            col("walk")
//          )
//          .as[RandomWalk]
//          .repartition(1)
//      }
//    }

    val df = walkStartDS
      .select(
        col("srcId"),
        col("walkNumber"),
        element_at(col("walk"), -2).as("walkStep1"),
        element_at(col("walk"), -1).as("walkStep2")
      )

    Range(1, walkLength - 1).foldLeft(df){
      (df, i) => {
        df
          .join(relRDS, col(s"walkStep$i") === col("rel_srcID") and col(s"walkStep${i + 1}") === col("rel_dstID"))
          .withColumn(s"walkStep${i+2}", element_at(col("dstNeighbourhood"), drawAliasUdf(col("j"), col("q")) + 1))
          .drop("q", "j", "rel_srcID", "rel_dstID", "dstNeighbourhood", "srcNeighbourhood")
          .repartition(1)
      }
    }
  }

}
