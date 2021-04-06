package mm.graph.embeddings.graph

import org.apache.spark.sql.{Dataset, SparkSession}

case class IndexedNode(nodeName: String, nodeId: Long) extends SimpleNode


object IndexedNode {
  def read[T](data: T)(implicit spark: SparkSession): Dataset[IndexedNode] = {
    import spark.implicits._
    data match {
      case path: String => spark.read.csv(path).as[IndexedNode]
      case relations: Seq[IndexedNode] => relations.toDS
      case _ => throw new Exception("Unsupported node data type")
    }
  }
}
