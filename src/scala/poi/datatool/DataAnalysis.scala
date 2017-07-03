package scala.poi.datatool

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by pi on 7/3/17.
  */
class DataAnalysis(read:String) {

  var getdata = new GetRandomData(read)

  /**
    * 从给定路径的数据集中获取用户商户评分
    * @param datapath 数据集路径
    * @param user 用户列名
    * @param item 商户列名
    * @param rate 评分列名
    * @param per 分析数据百分比
    * @return 数据表
    */
  def userItemRateAnalysis(datapath:String,user:String,item:String,rate:String,format:String,per:Double): DataFrame ={
    var format1 = getdata.getRawPercentData(datapath,per)
    if(format.equals("csv"))
      format1 = getdata.getCsvRawPercentData(datapath,"\t",per)
    val data = getdata.getUserItemRating(format1,user,item,rate)
    return data
  }

  /**
    * 求平均
    * @param datapath
    * @param user
    * @param item
    * @param rate
    * @param per
    * @return
    */
  def userItemAvgratingAnalysis(datapath:String,user:String,item:String,rate:String,per:Double):DataFrame = {
    val data = getdata.getUserItemAvgrating(getdata.getRawPercentData(datapath,per)
      ,user,item,rate)
    return data
  }

  /**
    * 计数并条件过滤
    * @param input 输入数据表
    * @param ob 过滤的列名
    * @param filter 过滤条件
    * @param num 过滤边界
    * @return 含有过滤列和计数列的数据表
    */
  def userItemRateFilterAnalysis(input:DataFrame, ob:String, filter:String, num:Int):DataFrame={
    if(filter.equals(">")){
      val data = getdata.getUserCheckinMoreThan(input,ob,num)
      return data
    }
    else if(filter.equals("<")){
      val data = getdata.getUserCheckinLessThan(input,ob,num)
      return data
    }
    else if(filter.equals("=")){
      val data = getdata.getUserCheckinEqualWith(input,ob,num)
      return data
    }
    else return null

  }

  /**
    * 分析数据表
    * @param input 数据表
    * @return 稀疏度结果
    */
  def analyseSparsity(input:DataFrame): String ={
    val usernum = input.groupBy("_1").count().count()
    val itemnum = input.groupBy("_2").count().count()
    val totalnum = input.count()
    val result = totalnum.toDouble/(usernum * itemnum)
    return "Sparsity is : "+ result+"%, "+"User "+usernum + " item "+itemnum +" total "+totalnum
  }



}
