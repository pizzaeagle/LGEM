package mm.graph.embeddings
package fastRP

import mm.graph.embeddings.graph.Relation
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object FastRPMain {

  def run()(implicit spark: SparkSession, configuration: Configuration): Unit = {
    import spark.implicits._

    val edges = Relation.readUndirected(config.relationsInputDir)

    val indexedNodes = edges
      .select("srcID")
      .distinct
      .withColumn("i", row_number().over(Window.orderBy("srcID")))
      .select("srcID", "i")
      .toDF("srcID", "i")
      .repartition(200)
      .cache

    val distinctPairs = edges
      .select("srcID", "dstID")
      .distinct
      .cache

    val reverseDegreeMatrix = new CoordinateMatrix(
      distinctPairs
        .groupBy("srcID")
        .agg(count(col("dstID")).as("count"))
        .join(indexedNodes, Seq("srcID"))
        .select(col("count").cast("int"), col("i").cast("int"))
        .rdd
        .map(
          row => MatrixEntry(row.getInt(1), row.getInt(1), 1.0 / row.getInt(0))
        )
    )

    val adjancencyMatrix = new CoordinateMatrix(
      distinctPairs
        .join(indexedNodes, Seq("srcID"))
        .withColumnRenamed("i", "srcIndex")
        .select("srcID", "dstID", "srcIndex")
        .join(indexedNodes.as("d"), col("dstID") === col("d.srcID"))
        .withColumnRenamed("i", "dstIndex")
        .select(col("srcIndex").cast("int"), col("dstIndex").cast("int"))
        .rdd
        .map(
          row => MatrixEntry(row.getInt(0), row.getInt(1), 1)
        )
    )



    edges.unpersist()
    indexedNodes.unpersist()
    distinctPairs.unpersist()
    val similarityMatrix = coordinateMatrixMultiply(reverseDegreeMatrix,adjancencyMatrix)
    val verySparseRandomProjectionMatrix = VerySparseRandomProjectionMatrix.createMatrix(indexedNodes, 10)
    val alphas = Seq(0.3, 0.5, 0.7)
    val embeddings = new CoordinateMatrix(
      Seq(MatrixEntry(verySparseRandomProjectionMatrix.numRows(), verySparseRandomProjectionMatrix.numCols(), 0)).toDS.rdd)

    val x = alphas.foldLeft((embeddings, verySparseRandomProjectionMatrix))(
      (srp, alpha) => {
        val m = coordinateMatrixMultiply(similarityMatrix,srp._2)
        (coordinateMatrixAdd(coordinateMatrixMultiplyByScalar(m, alpha),srp._1), m)
      }
    )
    x._1.toIndexedRowMatrix().rows.take(10).foreach(println)
    x._1.entries.toDF().write.mode("overwrite").json("tmp_to_delete")
  }

  def coordinateMatrixMultiply(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix):
  CoordinateMatrix = {
    val M_ = leftMatrix.entries.map({ case MatrixEntry(i, j, v) => (j, (i, v)) })
    val N_ = rightMatrix.entries.map({ case MatrixEntry(j, k, w) => (j, (k, w)) })

    val productEntries = M_
      .join(N_)
      .map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w)) })
      .reduceByKey(_ + _)
      .map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })

    new CoordinateMatrix(productEntries)
  }

  def coordinateMatrixMultiplyByScalar(matrix: CoordinateMatrix, scalar: Double): CoordinateMatrix =
  {
    new CoordinateMatrix(matrix.entries.map({ case MatrixEntry(i, j, v) => MatrixEntry(i, j, v * scalar) }))
  }

  def coordinateMatrixAdd(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix):
  CoordinateMatrix = {
    val M_ = leftMatrix.entries.map({ case MatrixEntry(i1, j1, v1) => ((i1, j1), v1) })
    val N_ = rightMatrix.entries.map({ case MatrixEntry(i1, j1, v1) => ((i1, j1), v1) })

    val sumEntries = M_
      .fullOuterJoin(N_)
      .map({ case ((i,j), (v1, v2)) => MatrixEntry(i, j, v1.getOrElse(0.0) + v2.getOrElse(0.0)) })

    new CoordinateMatrix(sumEntries)
  }

}
