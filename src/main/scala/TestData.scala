package mm.graph.embeddings

import graph.{IndexedNode, Relation}

object TestData {
  import spark.implicits._

  val nodes = Seq(IndexedNode("1", 1),
    IndexedNode("2", 2),
    IndexedNode("3", 3),
    IndexedNode("4", 4),
    IndexedNode("5", 5),
    IndexedNode("6", 6),
    IndexedNode("7", 7)
  )

  val relations = Seq(Relation(1, 2),
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
    Relation(7, 3))
}



