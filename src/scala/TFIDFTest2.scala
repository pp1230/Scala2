package scala

import org.apache.spark.ml.feature.{VectorAssembler, IDF, HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.{SparseVector, Matrices, Matrix}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable


/**
  * Created by pi on 16-11-15.
  * 使用Vector的foreachActive方法提取idf.
  * 最後生成的Map（特徵，idf）
  * 問題：特徵相同的idf被覆蓋值
  */
class TFIDFTest2 {

}

object TFIDFTest2 {
  def main(args: Array[String]) {
    println("----------TF-IDF----------")
    val spark = SparkSession.builder().appName("TF-IDF Test2")
      .master("local[*]").getOrCreate()
    import spark.implicits._
    val testDF = spark.read.json("/home/pi/Documents/DataSet/dataset/yelp_review_test.json")
    //val testDF = spark.read.json("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review.json")
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(testDF)
    //wordsData.show()
    val hashTF = new HashingTF().setInputCol("words").setOutputCol("tfFeatures").setNumFeatures(10000)
    val tfFeatures = hashTF.transform(wordsData)
    //tfFeatures.select("review_id","words","tfFeatures").foreach(println(_))
    val idf = new IDF().setInputCol("tfFeatures").setOutputCol("idfFeatures")

    val idfModel = idf.fit(tfFeatures)
    val allDF = idfModel.transform(tfFeatures)
    //allDF.show()
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
    val vectors = allDF.select("idfFeatures").rdd.map { case Row(v: Vector) => v }
    //vectors.foreach(println(_))
    //val mt:Matrix = Matrices.dense(vectors)
    //case class Feature(dimention: String, tf: Array[Double], idf: Array[Double])
    //    val doubleVectors = vectors.map(vec => spark.sparkContext.parallelize(vec.toArray))
    //    doubleVectors.foreach(r => println(r.collect()))
    //println(doubleVectors)

    //    val normalVec = vectors.collect()
    //    normalVec.foreach(println(_))
    //    val arr = vectors.map(v => v.foreachActive( (a, b) => b))
    //    println(arr)

    //start exmaple1
    //    val vecs = allDF.select("idfFeatures").rdd.map { case Row(v: Vector) => v.toArray}
    //    vecs.foreach(a => a.foreach(println(_)))
    //end exmaple1

    //start exmaple2
//    val vecs = allDF.select("idfFeatures").rdd.map { case Row(v: Vector) => v.toArray.max }
//    vecs.foreach(println(_))
    //end exmaple2

    //    val first = vecs.first()
    //    val n =first.foreachActive((a, b) => b)
    //    println("---------->"+first+"/"+n)

    //val max =allDF.select("idfFeatures").rdd.map{case Row(myInt:Int, myIntList:Array[Int], myVecList:Array[Double]) => myVecList.toArray.reduce( (a, b) => if (a > b) a else b  )}
    //max.foreach(println(_))

    //    var map: Map[Int,Double] = Map()
    //    //vectors.first().foreachActive((a,b) => println(a+"/"+b))
    //    vectors.first().foreachActive((a,b) => map += (a -> b))
    //    println(map.max)
    //udf((v: Vector) => v.toSparse.values.max)

    var maxValue: Map[Int, Double] = Map()
    var maps: Map[Int, Double] = Map()
    //How to create this object?
    //var seqValue = ""
    case class Features(id: Int, value: (Int, Double))
    //var features = new Features(0,(0,0))
    //var seqValue = (0,(0 -> 0.0))
    //    var seqValue = Seq(Int,(Int,Double))
    var i = 0
    //Attention to collect method. Execution is lazy when you are not using Action.
    val vectorCol = vectors.collect()
    println(vectorCol.length)
    vectorCol.map(vec => {
      vec.foreachActive((a, b) => {
        maps += (a -> b)
        //println(maps)
      })
      maxValue += maps.max
      //seqValue += (i,maps.max)+"|"
      maps = Map()
      i += 1
    })
    println(maxValue)
    //println(seqValue)

    //spark.sparkContext.parallelize(maxValue.toSeq,2).saveAsTextFile("/home/pi/Documents/DataSet/dataset/yelp_review_test_1117.json")

    //    val words = allDF.select("words").rdd.map { case Row(v: mutable.WrappedArray[String]) => v}
    //    words.foreach(println(_))
  }
}