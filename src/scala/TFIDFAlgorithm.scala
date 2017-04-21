package scala

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by pi on 16-11-7.
  */
class TFIDFAlgorithm {
 var test = "Test class"

}

object TFIDFAlgorithm{
  def main(args: Array[String]) {
    println("I miss you !")
    println(new TFIDFAlgorithm().test)
    println("It is a program about tf-idf algorithm. ")
    val spark = SparkSession.builder().appName("TF-IDF Test Program")
      .master("local[*]").getOrCreate()
    //初始化文档，0123表示不同
    val sentenceData = spark.createDataFrame(Seq(
      (0,"this is a test test"),
      (1,"this is another example"),
      (2,"hello world spark"),
      (3,"it is a sunny day but I get up late")
    )).toDF("label","sentence")
    sentenceData.show()
    val tokenizer = new Tokenizer().setInputCol("sentence")
      .setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.show()
    val hashingTF = new HashingTF().setInputCol("words")
      .setOutputCol("rawFeatures").setNumFeatures(10000)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.select("label","rawFeatures").foreach(r => println(r))

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaleData = idfModel.transform(featurizedData)
    rescaleData.select("label","words","features").take(4).foreach(println)
    rescaleData.show()
    import org.apache.spark.ml.linalg.Vector
    import org.apache.spark.sql.Row
    rescaleData.select("features").rdd.map { case Row(v: Vector) => v}.first

    //start1:get a value in df.
//    val value =rescaleData.select("label").rdd.map{case Row(i:Int)=> i}.first()
//    println("---------->"+value)
//    rescaleData.select("label").limit(1).show()
    //end1

  }
}

/*
[0,(1000,[170,281,373,586],[0.5108256237659907,0.22314355131420976,0.5108256237659907,1.8325814637483102])]
[1,(1000,[243,281,373,779],[0.9162907318741551,0.22314355131420976,0.5108256237659907,0.9162907318741551])]
[2,(1000,[48,105,150],[0.9162907318741551,0.9162907318741551,0.9162907318741551])]
[3,(1000,[79,83,128,170,281,318,329,495,605,959],[0.9162907318741551,0.9162907318741551,0.9162907318741551,0.5108256237659907,0.22314355131420976,0.9162907318741551,0.9162907318741551,0.9162907318741551,0.9162907318741551,0.9162907318741551])]
 */