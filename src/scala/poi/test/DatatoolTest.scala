package scala.poi.test

import scala.poi.datatool.{DataAnalysis, GetRandomData}

/**
  * Created by pi on 7/3/17.
  */
class DatatoolTest {

}

object GetRandomDataTest{
  def main (args: Array[String] ) {
    val randomdata = new GetRandomData("/home/pi/doc/dataset/")
    randomdata.outputYelpPecentData("textdata/yelp_academic_dataset_review.json",
      "outputdata/yelp_randomdata3",0.005)
  }
}

object DataAnalysisGowallaTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.analyseSparsity(analysis.userItemRateAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","csv",0.001))
    println(result)
  }
}

object DataAnalysisYelpTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.analyseSparsity(analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1))
    println(result)
  }
}

object DataAnalysisTest1{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",0.001),"_1",">",1).toDF("userid","count")
    val data = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",0.001).toDF("userid","itemid","starts")
    val output = data.join(filter,"userid").toDF("_1","_2","_3","_4")
    val result = analysis.analyseSparsity(output)
    println(result)
  }

}

object Testall {
  def main(args: Array[String]) {
    DataAnalysisGowallaTest.main(args)
    DataAnalysisTest1.main(args)
  }
}