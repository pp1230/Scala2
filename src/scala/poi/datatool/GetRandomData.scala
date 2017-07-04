package scala.poi.datatool

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by pi on 17-7-1.
  */
class GetRandomData(base:String) {

  var basedir = base
  val ss = SparkSession.builder().appName("Yelp Rating")
    .master("local[*]").getOrCreate()
  import ss.implicits._

  /**
    * 获得百分比yelp用户对商户评分数据，求平均
    *
    * @param readpath 不含base的路径
    * @param writepath
    * @param percent
    */

  def outputYelpPecentData(readpath:String, writepath:String, percent:Double): Unit ={

    var yelpRating = getRawPercentData(readpath,percent)
    val data = getUserItemAvgrating(yelpRating,"user_id","business_id","stars")
    data.show()
    data.repartition(1).write.mode("overwrite").csv(base+writepath)
  }

  /**
    * 读取给定路径的数据集，获得用户对商户评分并计算平均分，输出到指定路径
    *
    * @param readpath 不含base的路径
    * @param writepath
    * @param user
    * @param item
    * @param rate
    * @param per
    */
  def outputUserItemRatePecentData(readpath:String, writepath:String,
                                   user:String,item:String,rate:String,per:Double): Unit ={

    var yelpRating = getRawPercentData(readpath,per)
    val data = getUserItemAvgrating(yelpRating,user,item,rate)
    data.show()
    //data.repartition(1).write.mode("overwrite").csv(base+writepath)
    writeData(data, 1, writepath)
  }

  def writeData(input:DataFrame, par:Int, writepath:String): Unit ={
    input.repartition(par).write.mode("overwrite").csv(base+writepath)
  }

  /**
    * 获得百分比原始数据
    *
    * @param readpath 原始数据集路径
    * @param per 百分比
    * @return 数据表
    */
  def getRawPercentData(readpath:String, per:Double) :DataFrame = {
    var yelpRating = ss.read.json(basedir+readpath)
    val Array(training,testing) = yelpRating.randomSplit(Array(per,1-per))
    return training

  }

  def getCsvRawPercentData(readpath:String, sep:String, per:Double):DataFrame = {
    val data = ss.read.format("csv").option("sep",sep)
      .csv(basedir+readpath)
    val Array(training,testing) = data.randomSplit(Array(per,1-per))
    return training
  }

  /**
    * 获得用户对商户的评分数据，求平均分
    *
    * @param input
    * @param user
    * @param item
    * @param rate
    * @return 数据表
    */
  def getUserItemAvgrating(input:DataFrame, user:String, item:String, rate:String):DataFrame = {
    val inputdata = getUserItemRating(input,user,item,rate)
    val data = inputdata.groupBy("_1","_2").avg("_3")
          .toDF("_1","_2","_3")
    return data
  }

  /**
    * 根据某列计数
    * @param input
    * @param ob
    * @return
    */
  def getGroupbyCount(input:DataFrame,ob:String):DataFrame = {
    val data = input.groupBy(ob).count().toDF("_1","_2")
    return data
  }

  /**
    * 获得用户对商户的评分数据，不求平均分
    *
    * @param input 原始数据集表
    * @param user 原始数据集user列名
    * @param item 原始数据集item列名
    * @param rate 原始数据集rate列名
    * @return 数据表
    */
  def getUserItemRating(input:DataFrame, user:String, item:String, rate:String):DataFrame = {
    val select = input.select(user,item,rate)
    val indexer1 = new StringIndexer()
      .setInputCol(user)
      .setOutputCol("userid")
    val indexed1 = indexer1.fit(select).transform(select).sort("userid")
    //indexed1.show(10000)
    val indexer2 = new StringIndexer()
      .setInputCol(item)
      .setOutputCol("itemid")
    val indexed2 = indexer2.fit(indexed1).transform(indexed1).sort("userid","itemid")
    //indexed2.show(10000)
    val data = indexed2.select("userid","itemid",rate)
      .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,r(2).toString.toDouble))
      .toDF("_1","_2","_3")
    return data
  }

  def getUserItemlalon(input:DataFrame, user:String, item:String, la:String, lon:String):DataFrame = {
    val select = input.select(user,item,la,lon)
    val indexer1 = new StringIndexer()
      .setInputCol(user)
      .setOutputCol("userid")
    val indexed1 = indexer1.fit(select).transform(select).sort("userid")
    //indexed1.show(10000)
    val indexer2 = new StringIndexer()
      .setInputCol(item)
      .setOutputCol("itemid")
    val indexed2 = indexer2.fit(indexed1).transform(indexed1).sort("userid","itemid")
    //indexed2.show(10000)
    val data = indexed2.select("userid","itemid",la,lon)
      .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,
        r(2).toString.toDouble, r(3).toString.toDouble))
      .toDF("_1","_2","_3","_4")
    return data
  }

  /**
    * 对计数列进行过滤
    * @param input
    * @param ob
    * @param num
    * @return
    */
  def getUserCheckinMoreThan(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob)
    //select.show()
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _2 > "+num)
    return data
  }

  def getUserCheckinLessThan(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob)
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _2 < "+num)
    return data
  }

  def getUserCheckinEqualWith(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob)
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _2 = "+num)
    return data
  }


}
