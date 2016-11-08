package scala

import org.apache.spark.sql.SparkSession

/**
  * Created by pi on 16-8-13.
  */
class ScalaSQLTest {

}

object ScalaSQLTest{

  val ss = SparkSession.builder().appName("Spark SQL Test")
    .master("local[*]").getOrCreate()

  def main(args: Array[String]) {
    val df = ss.read.json("/home/pi/doc/Spark/json").toDF()
    df.show()
    df.printSchema()
    df.select("email").show()
    df.groupBy("firstName").count().show()

    df.createOrReplaceTempView("people")
    val sqldf = ss.sql("select * from people")
    sqldf.show()

  }


}
