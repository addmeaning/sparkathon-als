import org.apache.spark.sql.{DataFrame, SparkSession}
import  org.apache.spark.sql.functions._
object JoinDemo extends App {
  val spark = SparkSession.builder().appName("join-test").master("local[*]").getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")
  val recommendation: DataFrame = List(
    (0, 1, 2, 3),
    (1, 2, 3, 4),
    (2, 1, 3, 4)
  ).toDF("id", "rec1", "rec2", "rec3")

  recommendation.show()

  val movies = List(
    (1, "the Lord of the Rings"),
    (2, "Star Wars"),
    (3, "Star Trek"),
    (4, "Pulp Fiction")
  ).toDF("id", "name")
  spark.conf.set("spark.sql.crossJoin.enabled", "true")
  movies.show()

  val joined = recommendation
    .join(movies.select(col("id").as("id1"), 'name.as("n1")), 'id1 === recommendation.col("rec1"))
    .join(movies.select(col("id").as("id2"), 'name.as("n2")), 'id2 === recommendation.col("rec2"))
    .join(movies.select(col("id").as("id3"), 'name.as("n3")), 'id3  === recommendation.col("rec3"))
    .select('id, 'n1, 'n2, 'n3)
  joined.show()
}
