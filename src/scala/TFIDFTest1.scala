package scala

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession, Column}
import scala.collection.mutable
import scala.reflect.internal.util.TableDef.Column

/**
  * Created by pi on 16-11-14.
  */
class TFIDFTest1 {

}
case class Feature(id: String, dimention: String, tf: String, idf: String)

object TFIDFTest1{
  def main(args: Array[String]) {
    println("----------TF-IDF----------")
    val spark = SparkSession.builder().appName("TF-IDF Test")
      .master("local[*]").getOrCreate()
    import spark.implicits._
    val reviewDF = spark.read.json("/home/pi/Documents/DataSet/dataset/yelp_review_test.json")
    //val reviewDF = spark.read.json("/home/pi/Documents/DataSet/dataset/short_review.json")
    //reviewDF.show()
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(reviewDF)
    //wordsData.show()
    val hashTF = new HashingTF().setInputCol("words").setOutputCol("tfFeatures")
    val tfFeatures = hashTF.transform(wordsData)
    //tfFeatures.select("review_id","words","tfFeatures").foreach(println(_))
    val idf = new IDF().setInputCol("tfFeatures").setOutputCol("idfFeatures")
    val idfModel = idf.fit(tfFeatures)
    val idfFeatures1 = idfModel.transform(tfFeatures)
//    val idfVector = idfModel.idf
//    idfVector.toArray.foreach(println(_))
//    val uid = idfModel.uid
//    println(uid)
    //idfFeatures1.show()
    //idfFeatures1.select("review_id","words","idfFeatures").foreach(println(_))
    val idfFeatures2 = idfFeatures1.select("review_id","words","idfFeatures")
    //idfFeatures2.show()
//    val idfFeatures3 = idfFeatures2.select("idfFeatures")
//      .map(row => row.toString().split("\\["))
//
//    val seq = idfFeatures3.map(arr => {
//      val a1 = arr(1).replace("(","").replace("]","").replace(")","").replace(",","")
//      val a2 = arr(2).replace("(","").replace("]","").replace(")","")
//      val a3 = arr(3).replace("(","").replace("]","").replace(")","")
//      Feature(1,a1,a2,a3)
//    })
//    val idfFeatures4 = seq.toDF("id","featureNum","tf","idf")
//    seq.foreach(println(_))
//    idfFeatures4.foreach(println(_))
//    idfFeatures4.show()
//    val idfFeature5 = idfFeatures2.select("review_id","words")
//     // .map(feature => Feature(feature(0).toString.toInt,feature(1).toString.split(",").toSeq,feature(2).toString.split(",").toSeq))

    val review = idfFeatures2.select("review_id").collect()
    //review.foreach(println(_))
    var i = -1
    val totalFeatureDF = idfFeatures2.select("idfFeatures").map(row => row.toString().split("\\["))
      .map(arr =>{
        i += 1
        val a1 = arr(1).replace("(","").replace("]","").replace(")","").replace(",","")
        val a2 = arr(2).replace("(","").replace("]","").replace(")","")
        val a3 = arr(3).replace("(","").replace("]","").replace(")","")
        Feature(review(i).toString().replace("[","").replace("]",""),a1,a2,a3)
      }).toDF("review_id1","featureNum","tf","idf")
    //totalFeatureDF.show()

    val preDF = idfFeatures2.join(totalFeatureDF,$"review_id1" === $"review_id")
    preDF.show()

    //preDF.foreach(println(_))

    val result = preDF.select("words","idf").map{
     case Row(words:mutable.WrappedArray[String],idf:String) =>
        val key = words.map(_.replace(",","").replace(".",""))
        val value = idf.split(",")
        var map: Map[String,String] = Map()
       //println(key)
       //value.foreach(println(_))
       var j = 0
        for ( i <- key if j < value.size){
          map += (i->value(j))
          //println(i +"/"+value(j))
          j+=1
        }
        map.toList.sortBy(_._2)
    }
    result.foreach(m => println(m))
    //preDF.write.save("/home/pi/Documents/DataSet/dataset/yelp_review_test")
  }
}