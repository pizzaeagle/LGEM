package mm.graph.embeddings

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val rel = Seq((1,2),
      (1,3),
      (1,4),
      (2,2),
      (2,1),
      (2,3),
      (2,4),
      (3,7),
      (3,4),
      (5,7),
      (4,3),
      (4,1),
      (4,6),
      (6,5),
      (7,3)).toDF("start","dest")
    val walk = rel.toDF("0","1")

    val walks = (1 to 10).foldLeft(walk) { (walk, i) =>
      val w = Window.partitionBy($"0").orderBy(rand(123))
      walk.as("rel").join(rel, walk(i.toString) === rel("start"))
        .drop(col("start"))
        .withColumnRenamed("dest", (i+1).toString)
        .withColumn("rn", row_number.over(w))
        .where(col("rn") < 10)
        .drop("rn")
    }
    println(walks.count)
    walks.show(20)
    val x = walks.select(array((0 to 10).map(i => col(i.toString).cast("string")):_*).as("walks"))

    val word2vec = new Word2Vec().setInputCol("walks").fit(x)

    word2vec.getVectors.show()

    Thread.sleep(1000000)
  }

//  def randomWalk(relations: DataFrame): DataFrame = {
//    relations.join()
//  }
}
