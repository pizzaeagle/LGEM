package mm.graph.embeddings

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer
import java.net._
import java.io._

class Test extends FlatSpec with Matchers with BeforeAndAfterAll {
  val buffer = new ArrayBuffer[String]()

  "xxx" should "xxx" in {
    buffer.foreach(println)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val url = new URL("https://raw.githubusercontent.com/neo4j-examples/graph-embeddings/main/data/roads.csv")
    val in = new BufferedReader(new InputStreamReader(url.openStream))
    var inputLine = in.readLine
    while (inputLine != null) {
      if (!inputLine.trim.equals("")) {
        buffer += inputLine.trim
      }
      inputLine = in.readLine
    }
    in.close
  }
}
