package mm.graph.embeddings
package graph

import org.apache.spark.sql.{Dataset, SparkSession}

case class Graph(vertices: Dataset[Node], edges: Dataset[Relation])

object Graph {
  def loadUndirectedGraphFromCSV[T <: Node, V <: Relation](verticesPath: String, edgesPath: String)(
    implicit sparkSession: SparkSession
  ): Graph = {
    import sparkSession.implicits._
    val verticesDS = Node.read(verticesPath)(sparkSession)
    val edgesDS = Relation.readDirected(edgesPath)(sparkSession)
    Graph(verticesDS, edgesDS)
  }

  def loadUndirectedGraphFromLocalData[T <: Node, V <: Relation](vertices: Seq[T], edges: Seq[V])(
    implicit sparkSession: SparkSession
  ): Graph = {
    import sparkSession.implicits._
    val verticesDS = Node.read(vertices)(sparkSession)
    val edgesDS = Relation.readDirected(edges)(sparkSession)
    Graph(verticesDS, edgesDS)
  }
}
