package scala.poi.datatool

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{explode,lit,regexp_replace}
import org.apache.log4j.{Level, Logger}

import scala.poi.algorithm.LinearRegressionAl

/**
  * Created by pi on 17-7-1.
  */
class GetRandomData(base:String) {

  var basedir = base
  val ss = SparkSession.builder().appName("Yelp Rating")
    .master("local[*]").getOrCreate()
  import ss.implicits._
  Logger.getLogger("org").setLevel(Level.WARN)
//  var conf = new SparkConf().setAppName("Yelpdata").setMaster("local[*]")
//  var sc = new SparkContext(conf)
//  sc.setLogLevel("WARN")

  def getLibsvmData(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("libsvm")
    .load(basedir+path)
    return dataset
  }

  def textToVector(dataFrame: DataFrame,col: String):DataFrame={

    val input = dataFrame.withColumn("replace",regexp_replace(dataFrame.col(col),"\\p{Punct}",""))
    val token = new Tokenizer().setInputCol("replace").setOutputCol("token")
    val data = token.transform(input)
    val remover1 = new StopWordsRemover().setInputCol("token").setOutputCol("remove1").setStopWords(Array(" "))
    val remove1 = remover1.transform(data)
    val remover2 = new StopWordsRemover().setInputCol("remove1").setOutputCol("remove2")
    val remove2 = remover2.transform(remove1)
    val transformer = new CountVectorizer().setInputCol("remove2").setOutputCol("vector")
    val vector = transformer.fit(remove2).transform(remove2)
    return vector
  }

  def getTextData(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("text")
      .load(basedir+path)
    return dataset
  }

  def getJsonData(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("json")
      .load(basedir+path)
    return dataset
  }

  def getCsvData(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("csv")
      .load(basedir+path)
    return dataset
  }

  def getParquetData(path:String):DataFrame = {
    // Loads data.
    val dataset = ss.read.format("parquet")
      .load(basedir+path)
    return dataset
  }

  def userRegression(input:DataFrame,n:Int):String={

    val regression = new LinearRegressionAl()
//    var result = regression.transform(
//      input.filter($"user_id" === "2"), "topicDistribution", "s")
//    var result = sc.parallelize(Array("","")).toDF("topicDistribution","user_id","business_id","s")
    var vector = new DenseVector(Array(1,2,3,4,5))
    var data = List{(vector, "user","item",0,0)}
    var result = ss.createDataFrame(data).toDF("topicDistribution","user_id","business_id","s","prediction")
    println("---------result1-----------")
    result.show()
    input.select("user_id","s").distinct.collect().foreach(x => {
      val userid = x.getAs[String]("user_id")
      val rate = x.getAs[Int]("s")
      val selected = input.filter($"user_id" === userid)
      if(selected.count()>n) {
        val Array(training,testing) = selected.randomSplit(Array(0.7,0.3))
        if(training.count()>0) {
          val lr = regression.transform(testing,regression.fit(training,"topicDistribution","s"))
          val filter = rangeColume(lr,"prediction")
          result = result.union(filter)
        }
      }
      else {
        var vector = new DenseVector(Array(1,2,3,4,5))
        var data = List{(vector, userid,"business_id",rate,3.768)}
        var r = ss.createDataFrame(data).toDF("topicDistribution","user_id","business_id","s","prediction")
        result = result.union(r)
      }
    })
    println("---------result2-----------")
    result.show()
    return regression.evaluate(result)
  }


  def itemRegression(input:DataFrame,n:Int):String={

    val regression = new LinearRegressionAl()
    //    var result = regression.transform(
    //      input.filter($"user_id" === "2"), "topicDistribution", "s")
    //    var result = sc.parallelize(Array("","")).toDF("topicDistribution","user_id","business_id","s")
    var vector = new DenseVector(Array(1,2,3,4,5))
    var data = List{(vector, "user","item",0,0)}
    var result = ss.createDataFrame(data).toDF("topicDistribution","user_id","business_id","s","prediction")
    println("---------result1-----------")
    result.show()
    input.select("business_id","s").distinct.collect().foreach(x => {
      val business_id = x.getAs[String]("business_id")
      val rate = x.getAs[Int]("s")
      val selected = input.filter($"business_id" === business_id)
      if(selected.count()>n) {
        val Array(training,testing) = selected.randomSplit(Array(0.7,0.3))
        if(training.count()>0) {
          val lr = regression.transform(testing,regression.fit(training,"topicDistribution","s"))
          val filter = rangeColume(lr,"prediction")
          result = result.union(filter)
        }
      }
      else {
        var vector = new DenseVector(Array(1,2,3,4,5))
        var data = List{(vector, "user_id",business_id,rate,3.768)}
        var r = ss.createDataFrame(data).toDF("topicDistribution","user_id","business_id","s","prediction")
        result = result.union(r)
      }
    })
    println("---------result2-----------")
    result.show()
    return regression.evaluate(result)
  }

  def rangeColume(input:DataFrame, col:String):DataFrame={
    input.createOrReplaceTempView("table")
    val filter1 = ss.sql("select * from table where prediction >=1 and prediction <=5")
      .toDF("topicDistribution","user_id","business_id","s",col)
    val filter2 = ss.sql("select * from table where prediction <1 ")
      .toDF("topicDistribution","user_id","business_id","s","prediction<1")
    val filter3 = ss.sql("select * from table where prediction >5 ")
      .toDF("topicDistribution","user_id","business_id","s","prediction>5")
    val filter4 = filter2.withColumn(col, lit(1))
      .select("topicDistribution","user_id","business_id","s",col)
    val filter5 = filter3.withColumn(col, lit(5))
      .select("topicDistribution","user_id","business_id","s",col)
    return filter1.union(filter4).union(filter5)
  }

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

  def writeData(input:DataFrame, format:String, par:Int, writepath:String): Unit ={
    if(format.equals("csv"))
    input.repartition(par).write.mode("overwrite").csv(base+writepath)
    else if(format.equals("json"))
      input.repartition(par).write.mode("overwrite").json(base+writepath)
    else if(format.equals("parquet"))
      input.repartition(par).write.mode("overwrite").parquet(base+writepath)
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

  def getYelpUserFriendsTrustData(input:DataFrame, user:String, friends:String): DataFrame ={
    val data = input.select(user, friends)
    //data.show()
    val explodedata = data.withColumn(friends, explode($"friends"))
    //explodedata.show()
    val trust = explodedata.withColumn("trust", lit(1)).toDF("_1","_2","_3")
    //trust.show()
    //val result = getUserItemRating(trust,"user_id","friends","trust")
    return trust
  }


  /**
    * 将信任值转化为一
    *
    * @param input
    * @param user1
    * @param user2
    * @return
    */
  def transformToTrust1(input:DataFrame, user1:String, user2:String, trust:String):DataFrame ={
    val data = input.createOrReplaceTempView("table")
    val filter = ss.sql("select * from table where "+trust+" >0")
    //filter.show()
    val result = filter.select(user1,user2).withColumn("trust", lit(1))
    return result;
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

  def getUserItemAvg(input:DataFrame,user:String, item:String, rate:String):DataFrame = {
    val data = input.groupBy(user, item).avg(rate).toDF("_1","_2","_3")
    return data
  }
  /**
    * 根据某列计数
    *
    * @param input
    * @param ob
    * @return
    */
  def getGroupbyCount(input:DataFrame,ob:String):DataFrame = {
    val data = input.groupBy(ob).count().toDF("_1","_2")
    return data
  }

  /**
    * 根据两列计数
    *
    * @param input
    * @param ob1
    * @param ob2
    * @return
    */
  def getGroupbyCount(input:DataFrame,ob1:String,ob2:String):DataFrame = {
    val data = input.groupBy(ob1,ob2).count().toDF("_1","_2","_3")
    return data
  }

  def selectData(input:DataFrame, col1:String, col2:String, col3:String):DataFrame= {
    return input.select(col1,col2,col3).toDF("_1","_2","_3")
  }

  def selectData(input:DataFrame, col1:String, col2:String, col3:String, col4:String):DataFrame= {
    return input.select(col1,col2,col3,col4).toDF("_1","_2","_3","_4")
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
    val data = transformUseridandItemidOne(select, user, item, rate)
    return data
  }

  def getUserItemlalon(input:DataFrame, user:String, item:String, la:String, lon:String):DataFrame = {
    val select = input.select(user,item,la,lon)
    val data = transformUseridandItemidTwo(select,user,item, la, lon)
    return data
  }

  /**
    * transform userid and itemid into integer
    *
    * @param input input dataframe
    * @param user userid
    * @param item itemid
    * @return dataframe using integer id
    */
  def transformUseridandItemidOne(input:DataFrame, user:String, item:String, rate:String):DataFrame={
    val indexed1 = getIndexer(input,user).transform(input).sort(user+"(indexed)")
    val indexed2 = getIndexer(indexed1,item).transform(indexed1).sort(user+"(indexed)",item+"(indexed)")
    val data = indexed2.select(user+"(indexed)",item+"(indexed)",rate)
      .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,r(2).toString.toDouble))
      .toDF("_1","_2","_3")
    return data
  }

  def getIndexingData(input:DataFrame, indexer:StringIndexerModel):DataFrame={
      val result1 = indexer.transform(input).withColumnRenamed("_1(indexed)","_11(indexed)")
    val input2 = result1.withColumnRenamed("_1","_11").withColumnRenamed("_2","_1")
          .withColumnRenamed("_11","_2")
    val result2 = indexer.transform(input2).withColumnRenamed("_1(indexed)","_2(indexed)")
      .withColumnRenamed("_11(indexed)","_1(indexed)").select("_1(indexed)","_2(indexed)","_3")
          .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,r(2).toString.toDouble))
          .toDF("_1","_2","_3")
    //result2.show()
    return result2
  }

  def getIndexer(input:DataFrame,col: String): StringIndexerModel={
    val indexer = new StringIndexer()
      .setInputCol(col)
        .setOutputCol(col+"(indexed)")
    val result = indexer.fit(input)
    return result
  }

  def transformUseridandItemidTwo(input:DataFrame, user:String, item:String, la:String, lon:String):DataFrame={
    val indexed1 = getIndexer(input,user).transform(input).sort(user+"(indexed)")
    val indexed2 = getIndexer(indexed1,item).transform(indexed1).sort(user+"(indexed)",item+"(indexed)")
    val data = indexed2.select("userid","itemid",la,lon)
      .map(r=>(r(0).toString.toDouble.toInt,r(1).toString.toDouble.toInt,
        r(2).toString.toDouble, r(3).toString.toDouble))
      .toDF("_1","_2","_3","_4")
    return data
  }

  /**
    * 对一列进行group然后计数列进行过滤
    *
    * @param input
    * @param ob
    * @param num
    * @return
    */
  def getUserCheckinMoreThan(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob)
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

  def getUserTrustMoreThan(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = input
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where "+ob+" > "+num)
    return data
  }
  def getUserTrustEqualWith(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = input
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where "+ob+" = "+num)
    return data
  }
  def getUserTrustLessThan(input:DataFrame, ob:String, num:Int): DataFrame = {
    val select = input
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where "+ob+" < "+num)
    return data
  }

  /**
    * 对两列进行group然后对计数列进行过滤
    *
    * @param input
    * @param ob1
    * @param ob2
    * @param num
    * @return
    */
  def getUserItemCheckinMoreThan(input:DataFrame, ob1:String, ob2:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob1,ob2)
    //select.show()
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _3 > "+num)
    return data
  }

  def getUserItemCheckinLessThan(input:DataFrame, ob1:String, ob2:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob1,ob2)
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _3 < "+num)
    return data
  }

  def getUserItemCheckinEqualWith(input:DataFrame, ob1:String, ob2:String, num:Int): DataFrame = {
    val select = getGroupbyCount(input,ob1,ob2)
    select.createOrReplaceTempView("table")
    val data = ss.sql("select * from table where _3 = "+num)
    return data
  }

}
