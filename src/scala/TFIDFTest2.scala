package scala

import org.apache.spark.ml.feature.{VectorAssembler, IDF, HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * Created by pi on 16-11-15.
  */
class TFIDFTest2 {

}

object TFIDFTest2{
  def main(args: Array[String]) {
    println("----------TF-IDF----------")
    val spark = SparkSession.builder().appName("TF-IDF Test2")
      .master("local[*]").getOrCreate()
    import spark.implicits._
    val testDF = spark.read.json("/home/pi/Documents/DataSet/dataset/yelp_review_test.json")
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(testDF)
    //wordsData.show()
    val hashTF = new HashingTF().setInputCol("words").setOutputCol("tfFeatures")
    val tfFeatures = hashTF.transform(wordsData)
    //tfFeatures.select("review_id","words","tfFeatures").foreach(println(_))
    val idf = new IDF().setInputCol("tfFeatures").setOutputCol("idfFeatures")

    val idfModel = idf.fit(tfFeatures)
    val allDF = idfModel.transform(tfFeatures)
    allDF.show()
    val assambler = new VectorAssembler().setInputCols(Array("stars","idfFeatures")).setOutputCol("wordFeatures")
    val wordsFeatures = assambler.transform(allDF)
    wordsFeatures.show()
    val featureDF = wordsFeatures.select("wordFeatures")
    featureDF.foreach(println(_))
  }
}