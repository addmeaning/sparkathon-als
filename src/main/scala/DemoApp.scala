import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DemoApp extends SparkApp {

  val ratings = spark.read.option("header", true).option("inferSchema", true).csv("src/main/resources/small/ratings.csv")
  val anton = spark.read.option("header", true).option("inferSchema", true).csv("src/main/resources/small/anton.csv")

  import spark.implicits._

  val Array(training, test) = ratings.union(anton).randomSplit(Array(0.8, 0.2))

  // Build the recommendation model using ALS on the training data
  val als = new ALS()
    .setMaxIter(10)
    .setRegParam(0.2)
    .setRank(100)
    .setAlpha(0.1)
    .setColdStartStrategy("drop")
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")
  val model: ALSModel = als.fit(training)
  model.recommendForAllUsers(10)
  val predictions = model.transform(test)

  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")
  val rmse = evaluator.evaluate(predictions)
  println(s"Root-mean-square error = $rmse")

  // Generate top n movie recommendations for each user
  def recommend(n: Int, alsModel: ALSModel, movies: DataFrame): DataFrame = {
    val userRecs = alsModel.recommendForAllUsers(n)
    val range = 0 until n
    val recommendationIds = range.map(x => col("recommendations").getItem(x).getItem("movieId").as(s"rec$x")).toList
    val recs: String = range.map(x => s"'rec$x', rec$x").mkString("stack(10, ", ", ", ") as (rec, movie_id)")
    val recommendationUnfolded = userRecs.select(col("userId") :: recommendationIds: _*)
    recommendationUnfolded.select('userId, expr(recs))
      .where("rec is not null")
      .join(movies, col("movie_id") === movies.col("movieId"))
      .groupBy("userId")
      .pivot("rec")
      .agg(first("title"))
  }

  recommend(10, model, DatasetHelper.getSmallMovies).where('userId === 777).show()


  val pipeline = new Pipeline("als").setStages(Array(model))
  pipeline.write.overwrite().save("als-model")
}
