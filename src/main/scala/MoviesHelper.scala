
object MoviesHelper extends SparkApp {

  private def getMovies(path: String) = {
    val movies = spark.read.option("header", true).option("inferSchema", true).csv(path)
    import spark.implicits._
    movies.select('movieId, 'title)
  }
  def getSmallMovies = getMovies("src/main/resources/small/movies.csv")
  def getFullMovies = getMovies("src/main/resources/full/movies.csv")
}
