package scala

import java.sql.Timestamp

import breeze.numerics.exp
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by pi on 17-4-28.
  */
class ALSTime {

}

object ALSTime{
  case class RatingT(user_id: String, business_id: String, stars: String, time: String)
  case class FinalRatingT(user_id: Int, business_id: Int, stars: Float, time: Long)
  case class Rating(user_id: String, business_id: String, stars: String)
  case class FinalRating(user_id: Int, business_id: Int, stars: Float)
  case class Prediction(user_id: String, business_id: String, stars: String, prediction: String, time: String)
  case class FinalPrediction(user_id: Int, business_id: Int, stars: Float, prediction: Float, time: Long)
  case class csvResult(user_id: String, business_id: String, stars: String, prediction: String, residual:String, time: String)
  case class Result(user_id: Int, business_id: Int, stars: Float, prediction: Float, residual:Float, time: Float)
  def parseRaing(rating: RatingT):FinalRatingT = {
    val time = Timestamp.valueOf(rating.time+" 00:00:00").getTime
    FinalRatingT(rating.user_id.toFloat.toInt,rating.business_id.toFloat.toInt,rating.stars.toFloat, time)
  }
  def parseRaing(rating: Rating):FinalRating = {
    FinalRating(rating.user_id.toFloat.toInt,rating.business_id.toFloat.toInt,rating.stars.toFloat)
  }
  def parsePrediction(prediction:Prediction):FinalPrediction = {
    var p = prediction.prediction.toFloat
    if (p > 5.0)
      p = 5
    FinalPrediction(prediction.user_id.toFloat.toInt,prediction.business_id.toFloat.toInt
      ,prediction.stars.toFloat, p, prediction.time.toFloat.toLong)
  }
  def transform1(rating: RatingT):FinalRatingT = {
    FinalRatingT(rating.user_id.toFloat.toInt,rating.business_id.toFloat.toInt,rating.stars.toFloat,
      rating.time.toLong/10000000)
  }
  def transform2(rating: RatingT):FinalRatingT = {
    FinalRatingT(rating.user_id.toFloat.toInt,rating.business_id.toFloat.toInt,rating.stars.toFloat, rating.time.toFloat.toLong)
  }
  def transform3(prediction: Prediction):Result = {
    val s = prediction.stars.toFloat
    val p = prediction.prediction.toFloat
    val r = s - p
    Result(prediction.user_id.toFloat.toInt,prediction.business_id.toFloat.toInt,
      prediction.stars.toFloat, prediction.prediction.toFloat, r, (sigmoid((prediction.time.toFloat/100000-1.35)*20)).toFloat)
  }
  def transform4(prediction: csvResult):Result = {
    Result(prediction.user_id.toFloat.toInt,prediction.business_id.toFloat.toInt,
      prediction.stars.toFloat, prediction.prediction.toFloat, prediction.residual.toFloat, prediction.time.toFloat)
  }
  def sigmoid(inX:Double):Double= {
    return 1.0 / (1 + exp(-inX))
  }
  def main(args: Array[String]) {
    val ss = SparkSession.builder().appName("Yelp ALS Rating")
      .master("local[*]").getOrCreate()
    import ss.implicits._
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.recommendation.ALS

//        val yelpRating = ss.read.json("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review.json")
//              .select("user_id", "business_id", "stars","date")
//        //  .select( "user_id", "business_id", "stars").map(r=>Rating(r(0).toString,r(1).toString,r(2).toString)).map(parseRaing).toDF()
//        //val Array(yelpRating) = yelpRead.randomSplit(Array(0.0001))
//        yelpRating.show()
//        val indexer1 = new StringIndexer()
//          .setInputCol("user_id")
//          .setOutputCol("userid")
//        val indexed1 = indexer1.fit(yelpRating).transform(yelpRating).sort("userid")
//        //indexed1.show(10000)
//        val indexer2 = new StringIndexer()
//          .setInputCol("business_id")
//          .setOutputCol("bussinessid")
//        val indexed2 = indexer2.fit(indexed1).transform(indexed1).sort("userid","bussinessid")
//        //indexed2.show(10000)
//        val data = indexed2.select("userid","bussinessid","stars","date")
//          .map(r=>RatingT(r(0).toString,r(1).toString,r(2).toString,r(3).toString)).map(parseRaing).toDF()
//        data.show(100)
//        data.write.format("csv").save("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_matrix2.csv")

//    val readdata = ss.read.csv("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_matrix2.csv")
//    val avgdata = readdata.map(r=>RatingT(r(0).toString,r(1).toString,r(2).toString,r(3).toString)).map(transform1)
//      .groupBy("user_id","business_id").avg("stars","time").sort("user_id","business_id")
//    avgdata.show()
//    avgdata.write.format("csv").save("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_matrix3.csv")

//    val readdata = ss.read.csv("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_matrix3.csv")
//    val data = readdata.map(r=>RatingT(r(0).toString,r(1).toString,r(2).toString, r(3).toString))
//      .map(transform2).toDF()
//      data.show(1000)
//
//    val Array(training, testing) = data.randomSplit(Array(0.8,0.2))
//
//    val als = new ALS()
//      .setMaxIter(20)
//      .setRegParam(0.33)
//      .setUserCol("user_id")
//      .setItemCol("business_id")
//      .setRatingCol("stars")
//      .setNonnegative(true)
//      .setNumBlocks(15)
//
//    val model = als.fit(training)
//
//    // Evaluate the model by computing the RMSE on the test data
//    val predictions = model.transform(testing)
//
//    predictions.createOrReplaceTempView("rawprediction")
//    val drop = ss.sql("select * from rawprediction where prediction != 'NaN'").toDF()
//
//    val finalPrediction = drop.map(r => Prediction(r(0).toString, r(1).toString, r(2).toString, r(4).toString, r(3).toString))
//      .map(parsePrediction).toDF()
//    finalPrediction.show(1000)
//    val evaluator = new RegressionEvaluator()
//      .setMetricName("rmse")
//      .setLabelCol("stars")
//      .setPredictionCol("prediction")
//    val rmse = evaluator.evaluate(finalPrediction)
//    println(s"Root-mean-square error = $rmse")
//    finalPrediction.write.format("csv").save("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_result1.csv")

//    val readdata = ss.read.csv("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_result1.csv")
//    val result2 = readdata.map(r => Prediction(r(0).toString, r(1).toString, r(2).toString, r(3).toString, r(4).toString))
//        .map(transform3).toDF()
//    result2.show(1000)
//    result2.write.format("csv").save("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_resultSig.csv")

    val readdata = ss.read.csv("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_resultLine.csv")
      .map(r => csvResult(r(0).toString, r(1).toString, r(2).toString, r(3).toString, r(4).toString, r(5).toString)).map(transform4)
      .toDF("user_id","business_id","stars","predictionScore","residual","time")
    val colArray = Array("time")
    val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
    val vecDF: DataFrame = assembler.transform(readdata)
    val lr = new LinearRegression()
        .setMaxIter(30)
      .setRegParam(0.3)
      .setElasticNetParam(0.5)
      .setLabelCol("residual")
      .setFeaturesCol("features")
    val lrModel = lr.fit(vecDF)
    lrModel.extractParamMap()

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    val predictions = lrModel.transform(vecDF)
    predictions.selectExpr("residual", "round(prediction,1) as prediction").show

    val trainingSummary = lrModel.summary
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MAE: ${trainingSummary.meanAbsoluteError}")
  }
}