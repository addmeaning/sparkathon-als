import org.apache.spark.sql.SparkSession

trait SparkApp extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("sparkathon-als").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
}
