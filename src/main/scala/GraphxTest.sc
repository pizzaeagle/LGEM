
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col, concat, element_at, lit, monotonically_increasing_id}


implicit val spark: SparkSession = SparkSession
  .builder()
  .master("local[*]")
  .getOrCreate()
LogManager.getRootLogger.setLevel(Level.WARN)

import spark.implicits._

val df = (1 to 100).map(x => (x, Seq(x))).toDF("walk", "x")



val r = scala.util.Random

Range(1, 10).foldLeft(df) {
  (df, i) => {
    val x = df.withColumn("x", concat(col("x"), array(monotonically_increasing_id)))
    x.show(false)
    x
  }
}


