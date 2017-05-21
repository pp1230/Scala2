package scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by pi on 17-5-21.
  */
class WordCount {

}

object WordCount{
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("Scapi").setMaster("local[*]")
    //var conf = new SparkConf().setAppName("Scapi").setMaster("spark://192.168.1.104:4050")
    var sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val input = "a b c b a"
    val textinput = sc.textFile("/home/pi/Documents/CloudComputing/input.txt")

    //    val rdd1 = sc.parallelize(input.split(" "))
    //    val rdd2 = rdd1.map((_,1)).reduceByKey(_+_)
    //    val rdd3 = rdd2.map(_._1)
    //    val rdd4 = rdd3.cartesian(rdd3)
    //    val rdd5 = rdd4.join(rdd2)
    //    val rdd6 = rdd5.map(r => ((r._2._1,r._1),r._2._2))
    //    //rdd6.foreach(print(_))
    //    val rdd7 = rdd6.map(r => {
    //      if(r._1._1.equals(r._1._2))
    //        (r._1,1)
    //      else r})
    //    rdd7.foreach(print(_))

    val rdd1 = sc.parallelize(input.split(" ")).map((_, 1)).reduceByKey(_ + _)
    val rdd2 = rdd1.map(_._1)
    val rdd3 = rdd2.cartesian(rdd2)
    val rdd4 = rdd3.join(rdd1).map(r => ((r._2._1, r._1), r._2._2))
      .map(r => {
        if (r._1._1.equals(r._1._2))
          (r._1, 1)
        else r
      })
    rdd4.foreach(print(_))

  }
}
