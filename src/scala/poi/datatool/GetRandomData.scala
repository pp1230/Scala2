package scala.poi.datatool

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

/**
  * Created by pi on 17-7-1.
  */
class GetRandomData(readpath:String, writepath:String, per:Double) {

  var readPath:String = readpath
  var writePath:String = writepath
  var percent:Double = per
  val ss = SparkSession.builder().appName("Yelp Rating")
    .master("local[*]").getOrCreate()
  import ss.implicits._

  def getPecentData(): Unit ={

    var yelpRating = ss.read.json(readPath)
    //  .select( "user_id", "business_id", "stars").map(r=>Rating(r(0).toString,r(1).toString,r(2).toString)).map(parseRaing).toDF()
    val Array(training,testing) = yelpRating.randomSplit(Array(percent,1-percent))
    yelpRating.show()
    yelpRating = training
    val indexer1 = new StringIndexer()
      .setInputCol("user_id")
      .setOutputCol("userid")
    val indexed1 = indexer1.fit(yelpRating).transform(yelpRating).sort("userid")
    //indexed1.show(10000)
    val indexer2 = new StringIndexer()
      .setInputCol("business_id")
      .setOutputCol("bussinessid")
    val indexed2 = indexer2.fit(indexed1).transform(indexed1).sort("userid","bussinessid")
    //indexed2.show(10000)
    val data = indexed2.select("userid","bussinessid","stars")
      .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,r(2).toString.toDouble)).groupBy("_1","_2").avg("_3")
      .toDF("uid","bid","rate")
    data.show()
    data.repartition(4).write.mode("overwrite").csv(writePath)
  }


}
