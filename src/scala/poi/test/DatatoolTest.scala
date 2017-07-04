package scala.poi.test

import scala.poi.datatool.{DataAnalysis, GetRandomData}

/**
  * Created by pi on 7/3/17.
  */
class DatatoolTest {

}

/**
  * 随机抽取给定百分比的数据
  */
object GetRandomDataTest{
  def main (args: Array[String] ) {
    val randomdata = new GetRandomData("/home/pi/doc/dataset/")
    randomdata.outputYelpPecentData("textdata/yelp_academic_dataset_review.json",
      "outputdata/yelp_randomdata3",0.005)
  }
}

/**
  * 分析Gowalla数据的稀疏度
  */
object DataAnalysisGowallaTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.analyseSparsity(analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","_c3","csv",1))
    println("DataAnalysisGowallaTest"+result)
  }
}

/**
  * 分析yelp数据集的稀疏度
  */
object DataAnalysisYelpTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.analyseSparsity(analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1))
    println("DataAnalysisYelpTest"+result)
  }
}

/**
  * 按照用户的checkin数目过滤
  */
object DataAnalysisTest1{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1),"_1",">",10).toDF("userid","count")
    val data = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    val output = data.join(filter,"userid").toDF("_1","_2","_3","_4")
    val result = analysis.analyseSparsity(output)
    println("DataAnalysisTest1"+result)
  }

}

/**
  * 按照用户checkin数目过滤后输出
  */
object DataFilterYelpOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",10).toDF("userid","count")
    val data = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    val output = data.join(filter,"userid").toDF("_1","_2","_3","_4")
    analysis.outputResult(output, 1, "outputdata/YelpUserCheckinMorethan10")
  }
}

/**
  * 按照用户的checkin数目过滤
  */
object DataAnalysisGowallaFilter{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","csv",1),"_1",">",10).toDF("userid","count")
    val data = analysis.userItemRateAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","csv",1).toDF("userid","itemid","starts")
    val output = data.join(filter,"userid").toDF("_1","_2","_3","_4")
    val result = analysis.analyseSparsity(output)
    println("GowallaFilterTest"+result)
  }
}
/**
  * 按照用户checkin数目过滤后输出
  */
object DataFilterGowallaOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","_c3","csv",1),"_1",">",10).toDF("userid","count")
    val data = analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","_c3","csv",1).toDF("userid","itemid","la","lon")
    val output = data.join(filter,"userid").toDF("_1","_2","_3","_4","_5")
    analysis.outputResult(output,1,"outputdata/GowallaUserCheckinMoreThan10")
  }
}

object Testall {
  def main(args: Array[String]) {

    DataFilterYelpOutput.main(args)
    DataFilterGowallaOutput.main(args)
  }
}