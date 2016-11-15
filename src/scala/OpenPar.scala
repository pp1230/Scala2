package scala

import org.apache.spark.sql.SparkSession

/**
  * Created by pi on 16-11-14.
  */
class OpenPar {

}

object OpenPar{
  def main(args: Array[String]) {
    println("----------Parqute----------")
    val spark = SparkSession.builder().appName("par test")
      .master("local[*]").getOrCreate()
    val text = spark.read.parquet("/home/pi/Documents/DataSet/dataset/yelp_review_test_result/part-r-00000-6b555ea8-0c05-466f-b4c5-c8774d3b5745.snappy.parquet")
    text.show()
  }
}