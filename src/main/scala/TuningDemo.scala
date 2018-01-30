import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

object TuningDemo extends SparkApp {

  val Array(train, test) = DatasetHelper.getSmallRatings.randomSplit(Array(0.8, 0.2))
  val evaluators = for (rank <- Seq(100, 75);
                        regParam <- Seq(0.5, 0.25, 0.2);
                        alpha<- Seq(0.1, 5.0))
  yield {
    val model = new ALS()
      .setAlpha(alpha)
      .setRank(rank)
      .setRegParam(regParam)
      .setMaxIter(20)
      .setColdStartStrategy("drop")
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setPredictionCol("prediction").fit(train)

    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    (rmse, (rank, regParam, alpha))
  }

  evaluators.sorted.reverse.foreach(println)
}
