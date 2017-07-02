package scala

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  * Created by pi on 17-5-9.
  */
class BaseLinePredictor {

}
object BaseLinePredictor{
  case class FinalRating(userId: Int, businessId: Int, avgScore: Float)
  case class VoteRating(userId: Int, businessId: Int, avgScore: Float, votes: Float)
  case class PredictionRating(userId: Int, businessId: Int, residual: Float, prediction: Float)
  case class AvgRating(tuserId: Int, tbusinessId: Int, avgScore: Float, useravg: Float, busavg: Float)
  case class preAvgRating(tuserId: Int, tbusinessId: Int, avgScore: Float, useravg: String, busavg: String)
  def main(args: Array[String]) {

    def rating(avgRating: preAvgRating):AvgRating={
      AvgRating(avgRating.tuserId,avgRating.tbusinessId,avgRating.avgScore,avgRating.useravg.toFloat,avgRating.busavg.toFloat)
    }

//    val ss = SparkSession.builder().appName("Yelp Baseline Predictor")
//      .master("spark://172.31.34.14:6066").getOrCreate()
    val ss = SparkSession.builder().appName("Yelp Baseline Predictor")
        .master("local[*]").getOrCreate()
    import ss.implicits._
    val orderData = ss.read.csv("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review_startimevoteagg.csv")
    //orderData.createOrReplaceTempView("orderdata")
    //val data = ss.sql("select * from orderdata where _c4 > 10")
    val Array(training, testing) = orderData.randomSplit(Array(0.8,0.2))
    training.createOrReplaceTempView("training")
    testing.createOrReplaceTempView("testing")
    val pretestingData = ss.sql("select * from testing where _c0 in (select _c0 from training)")
    pretestingData.show()

    val pretrainingData = training.select("_c0","_c1","_c2","_c4").toDF("userId","businessId","avgScore","votes")
      .map(r => VoteRating(r(0).toString.toInt,r(1).toString.toInt,r(2).toString.toFloat,r(3).toString.toFloat))
    pretrainingData.createOrReplaceTempView("pretraindata")
    val trainingData = ss.sql("select * from pretraindata where votes >= 0")
      .sort("userId")
      .map(r => FinalRating(r(0).toString.toInt,r(1).toString.toInt,r(2).toString.toFloat))
    val alstrainingData = ss.sql("select * from pretraindata where votes >= 3")
      .sort("userId")
      .map(r => FinalRating(r(0).toString.toInt,r(1).toString.toInt,r(2).toString.toFloat))

    val testData = pretestingData.select("_c0","_c1","_c2")
      .map(r => FinalRating(r(0).toString.toInt,r(1).toString.toInt,r(2).toString.toFloat))
      .toDF("tuserId","tbusinessId","avgScore").sort("tuserId")

    trainingData.show(100)
    val useravg = trainingData.groupBy("userId").avg("avgScore").toDF("useravgId","useravg")
    val busavg = trainingData.groupBy("businessId").avg("avgScore").toDF("busavgId","busavg")
    useravg.show(100)

    /**
      * 仅Only use baseline predictor
      * Root-mean-square error = 1.2248886455945305
      * Mean-absolute-error = 0.9908542582098301
      */
    val preaddUserAvg = testData.join(useravg,$"useravgId" === $"tuserId","left")
      .join(busavg,$"busavgId" === $"tbusinessId","left")
    preaddUserAvg.show(100)

    val avgScore = pretrainingData.groupBy().avg("avgScore").first().get(0)
    println("avgScore:"+avgScore)
    val addUserAvg = preaddUserAvg.map(r => {
      var r4 = avgScore.toString
      var r6 = avgScore.toString
      if(r(4)==null&&r(6)!=null){
        r6 = r(6).toString
      }
      else if (r(4)!=null&&r(6)==null) {
        r4 = r(4).toString
      }
      else if (r(4)!=null&&r(6)!=null){
        r6 = r(4).toString
        r4 = r(6).toString
      }

        rating(preAvgRating(r(0).toString.toInt,r(1).toString.toInt,r(2).toString.toFloat
          ,r4,r6))
    })
    addUserAvg.show(100)

//    addUserAvg.createOrReplaceTempView("avg")
//    val testnull = ss.sql("select * from avg where tuserId = 'null' or tbusinessId = 'null'" +
//      " or avgScore = 'null' or useravg = 'null' or busavg= 'null'")
//    testnull.show()


    val testPrediction = addUserAvg.select(addUserAvg("tuserId"),addUserAvg("tbusinessId"),addUserAvg("avgScore")
      ,(addUserAvg("useravg")+addUserAvg("busavg")+avgScore)/3).toDF("puserId","pbusinessId","avgScore","prediction")
    testPrediction.show()
    val testResidual = testPrediction.select(testPrediction("puserId"),testPrediction("pbusinessId"),
      testPrediction("avgScore")- testPrediction("prediction")).toDF("userId","businessId","residual")
    testResidual.show()
    println("traingdata:"+trainingData.count()+" useravg:"+useravg.count()+" addUserAvg:"+addUserAvg.count()
      +" prediction:"+testPrediction.count()+ " testData:"+testData.count())

    //-------------Baseline predictor prediction-------------

    //    val evaluator = new RegressionEvaluator()
    //      .setMetricName("rmse")
    //      .setLabelCol("avgScore")
    //      .setPredictionCol("prediction")
    //    val rmse = evaluator.evaluate(testPrediction)
    //    println(s"Root-mean-square error = $rmse")
    //    evaluator.setMetricName("mae")
    //    val mae = evaluator.evaluate(testPrediction)
    //    println(s"Mean-absolute-error = $mae")

    //-------------------------------------------------------

    val traingTable = alstrainingData.join(useravg,$"userId" === $"useravgId","inner")
      .join(busavg,$"businessId" === $"busavgId","inner")

    val prediction = traingTable.select(traingTable("userId"),traingTable("businessId"),traingTable("avgScore")
      ,(traingTable("useravg")+traingTable("busavg")+avgScore)/3).toDF("userId","businessId","avgScore","prediction")
    prediction.show()

    val trainResidual = prediction.select(prediction("userId"),prediction("businessId"),
      prediction("avgScore")-prediction("prediction")).toDF("userId","businessId","residual")
    trainResidual.show()

    //val Array(training, testing) = residual.randomSplit(Array(0.8,0.2))

    val als = new ALS()
      .setMaxIter(20)
      .setRegParam(0.3)
      .setUserCol("userId")
      .setItemCol("businessId")
      .setRatingCol("residual")
      .setRank(500)

    val model = als.fit(trainResidual)

    // Evaluate the model by computing the RMSE on the test data
    val predictions = model.transform(testResidual)
    predictions.show(100)

    val result = predictions.map(r => {
      if(r(3).toString.equals("NaN")||r(3) == Float.NaN)
        PredictionRating(r(0).toString.toInt,r(1).toString.toInt,r(2).toString.toFloat,0.0f)
      else
        PredictionRating(r(0).toString.toInt,r(1).toString.toInt,r(2).toString.toFloat,r(3).toString.toFloat)
    })
    result.show(100)
    predictions.createOrReplaceTempView("rawprediction")
    val drop = ss.sql("select * from rawprediction where prediction != 'NaN'").toDF()
    println("prediction count: "+ predictions.count()+"result count: "+ result.count())
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("residual")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(result)
    println(s"Root-mean-square error = $rmse")
    evaluator.setMetricName("mae")
    val mae = evaluator.evaluate(result)
    println(s"Mean-absolute error = $mae")


  }
}