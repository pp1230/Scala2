package scala

import org.apache.spark.ml.feature.{VectorAssembler, IDF, HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.{SparseVector, Matrices, Matrix}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable


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
//    val assambler = new VectorAssembler().setInputCols(Array("stars","idfFeatures")).setOutputCol("wordFeatures")
//    val wordsFeatures = assambler.transform(allDF)
//    wordsFeatures.show()
//    val featureDF = wordsFeatures.select("wordFeatures")
//    featureDF.foreach(println(_))

//    val vectors = allDF.select("idfFeatures").rdd.map{
//      case Row(vector: Vector) =>
//        vector
//    }
//    vectors.foreach(println(_))
    import org.apache.spark.ml.linalg.Vector
    val vectors = allDF.select("idfFeatures").rdd.map { case Row(v: Vector) => v}
    vectors.foreach(println(_))
    //val mt:Matrix = Matrices.dense(vectors)
    //case class Feature(dimention: String, tf: Array[Double], idf: Array[Double])
//    val doubleVectors = vectors.map(vec => spark.sparkContext.parallelize(vec.toArray))
//    doubleVectors.foreach(r => println(r.collect()))
    //println(doubleVectors)
    val normalVec = vectors.collect()
    normalVec.foreach(println(_))
    val arr = normalVec.toList

    val words = allDF.select("words").rdd.map { case Row(v: mutable.WrappedArray[String]) => v}
    words.foreach(println(_))
  }
}