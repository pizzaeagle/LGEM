package mm.graph.embeddings
package node2vec

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer

case class Alias(j: Int, q: Double)

object Alias {
  def setupAlias(probs: Seq[Double]): Array[Alias] = {
    val K = probs.length
    val J = Array.fill(K)(0)
    val q = Array.fill(K)(0.0)

    val smaller = new ArrayBuffer[Int]()
    val larger = new ArrayBuffer[Int]()

    val sum = probs.sum
    probs.zipWithIndex.foreach { case (weight, i) =>
      q(i) = K * weight / sum
      if (q(i) < 1.0) {
        smaller.append(i)
      } else {
        larger.append(i)
      }
    }

    while (smaller.nonEmpty && larger.nonEmpty) {
      val small = smaller.remove(smaller.length - 1)
      val large = larger.remove(larger.length - 1)

      J(small) = large
      q(large) = q(large) + q(small) - 1.0
      if (q(large) < 1.0) smaller.append(large)
      else larger.append(large)
    }

    J.zip(q).map(x => Alias(x._1, x._2))
  }

  def setupAliasUdf: UserDefinedFunction =
    udf((probs: Seq[Double]) => {
      setupAlias(probs)
    })

  def drawAlias(J: Seq[Int], q: Seq[Double]): Int = {
    val K = J.length
    val kk = math.floor(math.random * K).toInt

    if (math.random < q(kk)) kk
    else J(kk)
  }

  def drawAliasUdf: UserDefinedFunction =
    udf((J: Seq[Int], q: Seq[Double]) => {
      drawAlias(J,q)
    })
}