package mm.graph.embeddings

import mm.graph.embeddings.graphSage.GraphSageMain
import mm.graph.embeddings.fastRP.FastRPMain
import mm.graph.embeddings.node2vec.Node2VecMain

object Main extends App {

  val t1 = System.nanoTime
  config.embeddingType match {
    case "node2Vec"  => Node2VecMain.run()
    case "graphSage" => GraphSageMain.run()
    case "fastRP"    => FastRPMain.run()
  }
  val duration = (System.nanoTime - t1) / 1e9d
  println(s"Execution time: $duration")

}
