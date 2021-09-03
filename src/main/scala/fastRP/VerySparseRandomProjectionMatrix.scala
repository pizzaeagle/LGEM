package mm.graph.embeddings
package fastRP

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, col, explode, lit, struct, udf}

import scala.util.Random

object VerySparseRandomProjectionMatrix {
  def returnProjectionValue(s: Double): Double = {
    Random.nextDouble() match {
      case x if x >= 0.0 && x < 1.0 / (2 * s)             => math.sqrt(s)
      case x if x >= 1.0 / (2 * s) && x < 1 - 1 / (2 * s) => 0
      case x if x > 1 - 1.0 / (2 * s) && x <= 1.0         => -math.sqrt(s)
    }
  }

  def projection: UserDefinedFunction =
    udf(() => {
      returnProjectionValue(3.0)
    })

  def createMatrix(indexedNodes: DataFrame, size: Int) = new CoordinateMatrix(
    indexedNodes
      .select(col("i").as("x"))
      .withColumn(
        "values",
        explode(array(Range(0, size).map(i => struct(lit(i).as("i"), projection().as("val"))): _*))
      )
      .where("values.val != 0")
      .select("x", "values.i", "values.val")
      .rdd
      .map(
        row => MatrixEntry(row.getInt(0), row.getInt(1), row.getDouble(2))
      )
  )
}
