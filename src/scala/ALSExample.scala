package scala


import org.apache.spark.sql.SparkSession

/**
  * Created by pi on 4/21/17.
  */
class ALSExample {

}
object ALSExample{

  case class Rating(userId: Int, movieId: Int, rating: Float)
  def main(args: Array[String]) {
    val ss = SparkSession.builder().appName("Yelp Rating")
      .master("local[*]").getOrCreate()
    import ss.implicits._
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.recommendation.ALS


    def parseRating(str: String): Rating = {
      val fields = str.split("::")
      assert(fields.size == 4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
    }

    val ratings = ss.read.textFile("/home/pi/Documents/sample_movielens_ratings.txt")
      .map(parseRating)
      .toDF()
    ratings.show()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
  }
}
