package scala

import org.apache.spark.sql.SparkSession
import spire.std.map

/**
  * Created by pi on 9/9/16.
  */
class YelpTest {

}

case class YelpStucture(name:String,stars:Double,state:String)

object YelpTest {
  def main(args: Array[String]) {
    //初始化SparkSession
    val ss = SparkSession.builder().appName("Yelp Test")
      .master("local[*]").getOrCreate()

    import ss.implicits._
    //获得json数据并生成DataFrame
    val yelpData = ss.read.json("/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_business.json")
    yelpData.show(20)
    //提取需要使用的列
    val yelpSimpleData = yelpData.select("name","stars","state")
    yelpSimpleData.show(20)
    //SQL测试，使用更加灵活的sql筛选数据
    val yelpSimpleView = yelpSimpleData.createOrReplaceTempView("yelpdata")
    val yelpSqlTest = ss.sql("select * from yelpdata where state = 'PA' order by stars DESC")
    yelpSqlTest.show(10)

    //计算不同地区的平均评分
    println("----------RDD Function----------")
    var count = yelpSimpleData.map(row => (row.getAs[String]("state"),1)).rdd.reduceByKey(_+_)
    var scoreMap = yelpSimpleData
      .map(row => (row.getAs[String]("state"),row.getAs[Double]("stars"))).rdd.reduceByKey(_+_)

    var countArray = count.collect()
    scoreMap.map(score => {
      var countNum = 0
      countArray.foreach(count => {

        if(count._1.equals(score._1)){
         countNum = count._2
        }
      })
      if(countNum != 0)
        (score._1,score._2/countNum)
      else score
    }).sortByKey().foreach(println(_))
    println("----------DF Function----------")
    //将state相同的行合并，并计算不同地区的平均评分
    var coundf = yelpSimpleData.groupBy($"state").avg("stars").show()

  }
}