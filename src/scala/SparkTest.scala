package scala

import org.apache.spark.{SparkContext, SparkConf}

import scala.math.random

/**
  * Created by pi on 16-8-8.
  */
class SparkTest {
  var conf = new SparkConf().setAppName("SparkTest")
  var sc = new SparkContext(conf)

}


object out {
  def main(args: Array[String]) {
    val spark = new SparkTest
    val n = 100000
    val count = spark.sc.parallelize(1 until n).map { i =>
      val x = random
      val y = random
      if(x*x + y*y < 1)1 else 0
    }.reduce(_+_)
    val pi = 4.0*count/n
    println(pi)
  }
}
