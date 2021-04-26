package mm.graph.embeddings
package fastRP

import mm.graph.embeddings.graph.Relation
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FastRPMain {

  def run()(implicit spark: SparkSession, configuration: Configuration): Unit = {
    import spark.implicits._

    val edges = Relation.readUndirected(config.relationsInputDir)

    val indexedNodes = edges
      .select("srcID")
      .distinct
      .rdd
      .zipWithIndex
      .map(
        row => (row._1(0).asInstanceOf[Long], row._2)
      )
      .toDF("srcID", "i")

    val reverseDegreeMatrix = new CoordinateMatrix(
      edges
        .select("srcID", "dstID")
        .distinct
        .groupBy("srcID")
        .agg(count(col("dstID")).as("count"))
        .join(indexedNodes, Seq("srcID"))
        .select(col("srcID").cast("long"), col("count").cast("long"), col("i").cast("long"))
        .rdd
        .map(
          row => MatrixEntry(row.getLong(2), row.getLong(2), 1.0 / row.getLong(1))
        )
    ).toBlockMatrix()

    val adjancencyMatrix = new CoordinateMatrix(
      edges
        .select("srcID", "dstID")
        .join(indexedNodes, Seq("srcID"))
        .withColumnRenamed("i", "srcIndex")
        .select("srcID", "dstID", "srcIndex")
        .join(indexedNodes.as("d"), col("dstID") === col("d.srcID"))
        .withColumnRenamed("i", "dstIndex")
        .select(col("srcIndex").cast("long"), col("dstIndex").cast("long"))
        .rdd
        .map(
          row => MatrixEntry(row.getLong(0), row.getLong(1), 1)
        )
    ).toBlockMatrix().cache()

    val similarityMatrix = reverseDegreeMatrix.multiply(adjancencyMatrix)
    val verySparseRandomProjectionMatrix = VerySparseRandomProjectionMatrix.createMatrix(indexedNodes, 10)
    val embeddings = Range(0, 10).foldLeft(verySparseRandomProjectionMatrix)((srp, _) => similarityMatrix.multiply(srp))
    embeddings.toIndexedRowMatrix().rows.take(10).foreach(println)
    embeddings.toIndexedRowMatrix().rows.toDF().write.mode("overwrite").csv(configuration.outputDir)
  }
}
