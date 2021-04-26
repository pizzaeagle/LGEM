package mm.graph.embeddings
package utils

import graph.{Graph, Node, Relation}

import scala.util.Random

object TestData {

  val nodes = Seq(
    Node("1", 1, Seq(0.32, 0.25, 0.45)),
    Node("2", 2, Seq(0.12, 0.94, 0.92)),
    Node("3", 3, Seq(0.10, 0.23, 0.30)),
    Node("4", 4, Seq(0.20, 0.71, 0.27)),
    Node("5", 5, Seq(0.30, 0.34, 0.34)),
    Node("6", 6, Seq(0.40, 0.56, 0.76)),
    Node("7", 7, Seq(0.50, 0.98, 0.67))
  )

  val relations = Seq(
    Relation(1, 2),
    Relation(1, 3),
    Relation(1, 4),
    Relation(2, 2),
    Relation(2, 1),
    Relation(2, 3),
    Relation(2, 4),
    Relation(3, 7),
    Relation(3, 4),
    Relation(5, 7),
    Relation(4, 3),
    Relation(4, 1),
    Relation(4, 6),
    Relation(6, 5),
    Relation(7, 3)
  )

  def testGraphGeneration(nodesNumber: Int, relationPerNode: Option[Int], numberOfFeatures: Int) = {
    val data = Range(0, nodesNumber)
      .map { nodeID =>
        {
          val nodeName = java.util.UUID.randomUUID.toString
          val features = Range(0, numberOfFeatures).map(_ => Random.nextDouble())
          val node = Node(nodeName, nodeID, features)
          val relations = Range(0, relationPerNode.getOrElse(Random.nextInt(15))).map(_ => {
            val neighbour = Math.abs(Random.nextLong()) % nodesNumber
            Relation(nodeID, neighbour)
          })
          (node, relations)
        }
      }
      .foldLeft(Seq.empty[Node], Seq.empty[Relation]) { (data, rel) =>
        {
          (data._1 :+ rel._1, data._2 ++ rel._2)
        }
      }
    Graph.loadUndirectedGraphFromLocalData(data._1, data._2)
  }
}
