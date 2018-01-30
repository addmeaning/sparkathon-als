import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DatasetHelper extends SparkApp {

  def getSmallMovies = getMovies("src/main/resources/small/movies.csv")

  def getFullMovies = getMovies("src/main/resources/full/movies.csv")

  def getSmallRatings = getRatings("src/main/resources/small/ratings.csv")

  def getFullRatings = getRatings("src/main/resources/full/ratings.csv")

  private def getMovies(path: String) = {
    val movies: DataFrame = getDF(path)
    movies.select(col("movieId"), col("title"))
  }

  private def getRatings(path: String) = {
    val ranks: DataFrame = getDF(path)
    ranks.select(col("userId"), col("movieId"), col("rating"))
  }

  private def getDF(path: String) = {
    val movies = spark.read.option("header", true).option("inferSchema", true).csv(path)
    movies
  }

}
