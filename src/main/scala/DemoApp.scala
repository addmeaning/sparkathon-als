import org.apache.spark.ml.{Model, Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DemoApp extends SparkApp {
  import spark.implicits._

  //Dane
  val ratings = spark.read.option("header", true).option("inferSchema", true).csv("src/main/resources/small/ratings.csv")
  val anton = spark.read.option("header", true).option("inferSchema", true).csv("src/main/resources/small/anton.csv")


  private val input = ratings.union(anton)



  // Build the recommendation model using ALS on the training data
  val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.01)
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")

  private val pipeline: Pipeline = new Pipeline("als").setStages(Array(als))

  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")

  val paramGrid = new ParamGridBuilder()
    .addGrid(als.regParam, Array(0.1, 0.01, 0.001))
    .addGrid(als.maxIter, Array(5, 10, 20))
    .build()

  val crossValidator = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)
  private val model = crossValidator.fit(input).bestModel

    model.params.foreach(println)
  // Generate top 10 movie recommendations for each user
  def recommend(n: Int, alsModel: ALSModel, movies: DataFrame): DataFrame = {
    val userRecs = alsModel.recommendForAllUsers(10)

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

//  recommend(10, model, MoviesApp.getFullMovies).where('userId === 777).show()
  //  private val movies: DataFrame = MoviesApp.getSmallMovies
  //  private val value = (0 until 10)
  //    .map(x => col("recommendations").getItem(x).getItem("movieId").as(s"rec$x")).toList
  //  private val recs: String = (0 until 10).map(x => s"'rec$x', rec$x").mkString("stack(10, ", ", ", ") as (rec, movie_id)")
  //  private val recommendationUnfolded = userRecs.select(col("userId") :: value: _*)
  //  recommendationUnfolded.select('userId, expr(recs))
  //    .where("rec is not null")
  //    .join(movies, col("movie_id") === movies.col("movieId"))
  //    .groupBy("userId")
  //    .pivot("rec")
  //    .agg(first("title")).show()

    pipeline.write.overwrite().save("als-model")
}
