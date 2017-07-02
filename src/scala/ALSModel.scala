package scala

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
/**
  * Created by pi on 4/21/17.
  */
class ALSModel {

}
case class Rating(user_id: String, business_id: String, stars: String)
case class FinalRating(user_id: Int, business_id: Int, stars: Float)
case class Prediction(user_id: String, business_id: String, stars: String, prediction: String)
case class FinalPrediction(user_id: Int, business_id: Int, stars: Float, prediction: Float)
object ALSModel{
  def parseRaing(rating: Rating):FinalRating = {
    FinalRating(rating.user_id.toFloat.toInt,rating.business_id.toFloat.toInt,rating.stars.toFloat)
  }
  def parsePrediction(prediction:Prediction):FinalPrediction = {
    var p = prediction.prediction.toFloat
    if (p > 5.0)
      p = 5
    FinalPrediction(prediction.user_id.toFloat.toInt,prediction.business_id.toFloat.toInt
      ,prediction.stars.toFloat, p)
  }
  def main(args: Array[String]) {
    //初始化SparkSession
    val ss = SparkSession.builder().appName("Yelp Rating")
      .master("local[*]").getOrCreate()
    import ss.implicits._

//    val yelpRating = ss.read.json("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review.json")
//    //  .select( "user_id", "business_id", "stars").map(r=>Rating(r(0).toString,r(1).toString,r(2).toString)).map(parseRaing).toDF()
//    //val Array(yelpRating) = yelpRead.randomSplit(Array(0.0001))
//    yelpRating.show()
//    val indexer1 = new StringIndexer()
//      .setInputCol("user_id")
//      .setOutputCol("userid")
//    val indexed1 = indexer1.fit(yelpRating).transform(yelpRating).sort("userid")
//    //indexed1.show(10000)
//    val indexer2 = new StringIndexer()
//      .setInputCol("business_id")
//      .setOutputCol("bussinessid")
//    val indexed2 = indexer2.fit(indexed1).transform(indexed1).sort("userid","bussinessid")
//    //indexed2.show(10000)
//    val data = indexed2.select("userid","bussinessid","stars")
//      .map(r=>Rating(r(0).toString,r(1).toString,r(2).toString)).map(parseRaing).groupBy("user_id","business_id").avg("stars")
//    data.show(10000)
//    data.write.format("csv").save("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_matrix1.csv")


    // Build the recommendation model using ALS on the training data
    val readdata = ss.read.csv("./data/yelp_academic_dataset_review_matrix1.csv/")
//    readdata.createOrReplaceTempView("rawdata")
//    val newdata = ss.sql("select * from rawdata where _c2 <=5.0 and _c2 >= 0.0")
//    newdata.show()
    val data = readdata
      .map(r=>Rating(r(0).toString,r(1).toString,r(2).toString))
      .map(parseRaing).toDF()
//    data.show(100)
    val Array(training, testing) = data.randomSplit(Array(0.8,0.2))
//    training.show(1000)
//    testing.show(1000)
//    var i = 10
//    while(i < 100) {
      val als = new ALS()
        .setMaxIter(20)
        .setRegParam(0.33)
        .setUserCol("user_id")
        .setItemCol("business_id")
        .setRatingCol("stars")
        //.setNonnegative(true)
        .setRank(150)

      val model = als.fit(training)

      // Evaluate the model by computing the RMSE on the test data
      val predictions = model.transform(testing)
//      predictions.sort("user_id").show()
      predictions.createOrReplaceTempView("rawprediction")
      val drop = ss.sql("select * from rawprediction where prediction != 'NaN'").toDF()
      //    drop.createOrReplaceTempView("updatescore")
      //    val updateScore = ss.sql("update updatescore set prediction = 5.0 where prediction > 5.0")
      //    updateScore.show()
      val finalPrediction = drop.map(r => Prediction(r(0).toString, r(1).toString, r(2).toString, r(3).toString))
        .map(parsePrediction).toDF()
      finalPrediction.show(1000)
      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("stars")
        .setPredictionCol("prediction")
      val rmse = evaluator.evaluate(finalPrediction)
      println(s"Root-mean-square error = $rmse")

      evaluator.setMetricName("mse")
      val mse = evaluator.evaluate(finalPrediction)
      println(s"Mean-square error = $mse")

      evaluator.setMetricName("r2")
      val r2 = evaluator.evaluate(finalPrediction)
      println(s"r2 = $r2")

      evaluator.setMetricName("mae")
      val mae = evaluator.evaluate(finalPrediction)
      println(s"MAD--Mean Absolute Difference = $mae")
//      i+=5
//    }
  }
}