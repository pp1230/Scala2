package scala

import java.sql.Timestamp

import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql._

import scala.ALSTime.{csvResult, RatingT}

/**
  * Created by pi on 17-4-30.
  */
class ALSVote {

}

object ALSVote{
  case class DataStructure(user_id: String, business_id: String, stars: String, time: String, votes: String)
  case class PredictionStructure(user_id: String, business_id: String, stars: String, time: String, votes: String, prediction: String)
  case class CsvStructure(user_id: String, business_id: String, stars: String, prediction: String, time: String, votes:String, residual:String)
  case class FinalPrediction(user_id: Int, business_id: Int, stars: Float, time: Long, votes:Float, prediction: Float)
  case class Result(user_id: Int, business_id: Int, stars: Float, prediction: Float, time: String, votes:Float, residual:Float)
  case class VoteRating(user_id: String, business_id: String, stars: Double, time: Long , votes: Int)
  case class IndexedVoteRating(user_id: Int, business_id: Int, stars: Double, time: String , votes: Int)
  def CalculateVote(datastructure: DataStructure): VoteRating = {
    val time = Timestamp.valueOf(datastructure.time+" 00:00:00").getTime
    var i = 0
    var sum = 0
    while (i < 3) {
      val num = datastructure.votes.replace("[", "").replace("]", "").split(",")(i).toInt
      sum += num
      i += 1
    }
    VoteRating(datastructure.user_id, datastructure.business_id, datastructure.stars.toDouble, time, sum)

  }
  def Indexing(dataStructure: DataStructure):IndexedVoteRating = {
    IndexedVoteRating(dataStructure.user_id.toFloat.toInt,dataStructure.business_id.toFloat.toInt, dataStructure.stars.toDouble
    , dataStructure.time, dataStructure.votes.toFloat.toInt)
  }
  def parsePrediction(prediction:PredictionStructure):FinalPrediction = {
    var p = prediction.prediction.toFloat
    if (p > 5.0)
      p = 5
    FinalPrediction(prediction.user_id.toFloat.toInt,prediction.business_id.toFloat.toInt
      ,prediction.stars.toFloat, prediction.time.toFloat.toLong, prediction.votes.toFloat,p)
  }
  //个体get absolute value of residual
  def absolute(prediction: PredictionStructure):Result = {
    val s = prediction.stars.toFloat
    val p = prediction.prediction.toFloat
    var r = s - p
    if(r<0)r = -r
    Result(prediction.user_id.toFloat.toInt,prediction.business_id.toFloat.toInt,
      prediction.stars.toFloat, prediction.prediction.toFloat,prediction.time,
        prediction.votes.toFloat, r)
  }
  def devide(prediction: PredictionStructure):Result = {
    val s = prediction.stars.toFloat
    var p = prediction.prediction.toFloat
    if(p == 0)p+=0.0000001f
    var r = s/p
    if(r<0)r = -r
    Result(prediction.user_id.toFloat.toInt,prediction.business_id.toFloat.toInt,
      prediction.stars.toFloat, prediction.prediction.toFloat,prediction.time,
      prediction.votes.toFloat, r)
  }
  def prediction(prediction: CsvStructure):Result = {
    Result(prediction.user_id.toFloat.toInt,prediction.business_id.toFloat.toInt,
      prediction.stars.toFloat, prediction.prediction.toFloat,prediction.time,
      prediction.votes.toFloat, prediction.residual.toFloat)
  }
  def main(args: Array[String]) {
    val ss = SparkSession.builder().appName("Yelp ALS Rating")
      .master("local[*]").getOrCreate()
    import ss.implicits._
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.recommendation.ALS

    //transform votes, starts and time into numberic value and agg
//    val yelpRating = ss.read.json("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review.json")
////val yelpRating = ss.read.json("/home/pi/Documents/DataSet/dataset/yelp_review_test.json")
//      .select("user_id", "business_id", "stars","date","votes")
//    val voteData = yelpRating.map(r => DataStructure(r(0).toString, r(1).toString,r(2).toString,r(3).toString, r(4).toString))
//      .map(CalculateVote)
//
//    val indexer1 = new StringIndexer()
//      .setInputCol("user_id")
//      .setOutputCol("userid")
//    val indexed1 = indexer1.fit(voteData).transform(voteData).sort("userid")
//    //indexed1.show(10000)
//    val indexer2 = new StringIndexer()
//      .setInputCol("business_id")
//      .setOutputCol("bussinessid")
//    val indexed2 = indexer2.fit(indexed1).transform(indexed1).sort("userid","bussinessid")
//    //indexed2.show(10000)
//    val data = indexed2.select("userid","bussinessid","stars","time","votes")
//      .map(r=>DataStructure(r(0).toString,r(1).toString,r(2).toString,r(3).toString, r(4).toString)).map(Indexing)
//      .groupBy("user_id","business_id").avg("stars","time","votes").sort("user_id","business_id")
//    data.show(100)
//    data.write.format("csv").save("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_startimevoteagg.csv")

    //make prediction using user-item matrix based on ALS model
        val readdata = ss.read.csv("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_startimevoteagg.csv")
        val data = readdata.map(r=>DataStructure(r(0).toString,r(1).toString,r(2).toString, r(3).toString, r(4).toString))
          .map(Indexing).toDF()
          //data.show(1000)

        val Array(training, testing) = data.randomSplit(Array(0.8,0.2))

        val als = new ALS()
          .setMaxIter(20)
          .setRegParam(0.33)
          .setUserCol("user_id")
          .setItemCol("business_id")
          .setRatingCol("stars")
          .setNonnegative(true)
          .setNumBlocks(15)

        val model = als.fit(training)

    model.userFactors.sort("id").write.format("json").save("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_userfeatures.csv")
    model.itemFactors.sort("id").write.format("json").save("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_itemfeatures.csv")
    println("rank:"+model.rank)


        // Evaluate the model by computing the RMSE on the test data
//        val predictions = model.transform(testing)
//
//        predictions.createOrReplaceTempView("rawprediction")
//        val drop = ss.sql("select * from rawprediction where prediction != 'NaN'").toDF()
//
//        val finalPrediction = drop.map(r => PredictionStructure(r(0).toString, r(1).toString, r(2).toString,
//          r(3).toString, r(4).toString, r(5).toString))
//          .map(parsePrediction).toDF()
//        finalPrediction.show(1000)
//        val evaluator = new RegressionEvaluator()
//          .setMetricName("rmse")
//          .setLabelCol("stars")
//          .setPredictionCol("prediction")
//        val rmse = evaluator.evaluate(finalPrediction)
//        println(s"Root-mean-square error = $rmse")
//        finalPrediction.write.format("csv").save("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_voteresult1.csv")

    //transform residual into certain value
//        val readdata = ss.read.csv("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_voteresult1.csv")
//        val result2 = readdata.map(r => PredictionStructure(r(0).toString, r(1).toString, r(2).toString,
//          r(3).toString, r(4).toString, r(5).toString))
//            .map(devide).toDF()
//        result2.show(1000)
//        result2.write.format("csv").save("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_voteresultdiv.csv")

    //useing linear regression to calculate error
//    val readdata = ss.read.csv("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_voteresultab.csv")
//      .map(r => CsvStructure(r(0).toString, r(1).toString, r(2).toString, r(3).toString, r(4).toString, r(5).toString, r(6).toString)).map(prediction)
//      .toDF("user_id","business_id","stars","predictionScore","time","votes","residual")
//      .select("user_id","business_id","stars","predictionScore","votes","residual")
//    val colArray = Array("votes")
//    val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
//    val vecDF: DataFrame = assembler.transform(readdata)
//    val lr = new LinearRegression()
//      .setMaxIter(30)
//      .setRegParam(0.3)
//      .setElasticNetParam(0.5)
//      .setLabelCol("residual")
//      .setFeaturesCol("features")
//    val lrModel = lr.fit(vecDF)
//    lrModel.extractParamMap()
//
//    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
//    val predictions = lrModel.transform(vecDF)
//    predictions.selectExpr("residual", "round(prediction,1) as prediction").show
//
//    val trainingSummary = lrModel.summary
//    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
//    println(s"MAE: ${trainingSummary.meanAbsoluteError}")
  }
}