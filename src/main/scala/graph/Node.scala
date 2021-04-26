package mm.graph.embeddings.graph

import org.apache.spark.sql.{Dataset, SparkSession}

case class Node(nodeName: String, nodeId: Long, features: Seq[Double] = Seq.empty[Double])

object Node {
  def read[T](data: T)(implicit spark: SparkSession): Dataset[Node] = {
    import spark.implicits._
    data match {
      case path: String         => spark.read.csv(path).as[Node]
      case relations: Seq[Node] => relations.toDF.as[Node]
      case _                    => throw new Exception("Unsupported node data type")
    }
  }
}
