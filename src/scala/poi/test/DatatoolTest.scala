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
  * 输出yelp数据集的用户商家和评分
  */
object DataAnalysisYelpOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1)
    result.show()
    analysis.outputResult(result, 1, "outputdata/YelpUserItemRatingAll")
  }
}

object DataAnalysisYelpTrustOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val result = analysis.userandFriendTrustAnalysis("textdata/yelp_academic_dataset_user.json",
      "user_id","friends", 1)
    result.show()
    analysis.outputResult(result, 1, "outputdata/DataAnalysisYelpUserItemTrust")
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
  * 按照用户和商家checkin数目过滤后分析输出
  */
//object DataFilterYelpUserItemOutput{
//  def main(args: Array[String]) {
//    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
//    val filter = analysis.userItemRateFilterAnalysis(
//      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
//        "user_id","business_id","stars","json",1),"_1","_2",">",10).toDF("userid","itemid","count")
//    val data = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
//      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
//    val output = data.join(filter,Seq("userid","itemid")).toDF("_1","_2","_3","_4")
//    val result = analysis.analyseSparsity(output)
//    println("DataFilterYelpUserItemOutput"+result)
//    analysis.outputResult(output, 1, "outputdata/YelpUserItemCheckinMorethan10")
//  }
//}

/**
  * 按照用户和商家checkin数目过滤后分析输出
  */
object DataFilterYelpUserandItemOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",10).toDF("userid","count")
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_2",">",10).toDF("itemid","count")
    val data = analysis.userItemRateAnalysis("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    val output = data.join(filter1,"userid").join(filter2,"itemid")toDF("_1","_2","_3","_4","_5")
    val result = analysis.analyseSparsity(output)
    println("DataFilterYelpUserandItemOutput"+result)
    analysis.outputResult(output, 1, "outputdata/YelpUserandItemCheckinMorethan10")
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
  * 按照地点的checkin数目过滤
  */
object DataAnalysisGowallaItemFilter{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","csv",1),"_2",">",10).toDF("itemid","count")
    val data = analysis.userItemRateAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","csv",1).toDF("userid","itemid","starts")
    val output = data.join(filter,"itemid").toDF("_1","_2","_3","_4")
    val result = analysis.analyseSparsity(output)
    println("DataAnalysisGowallaItemFilter"+result)
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


object DataFilterGowallaUserandItemOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","_c3","csv",1),"_1",">",10).toDF("userid","count")
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","_c3","csv",1),"_2",">",10).toDF("itemid","count")
    val data = analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","_c3","csv",1).toDF("userid","itemid","la","lon")
    val output1 = data.join(filter1,"userid")
    val output2 = filter2.join(output1,"itemid").toDF("_1","_2","_3","_4","_5","_6")
    val result = analysis.analyseSparsity(output2)
    println("DataFilterGowallaUserandItemOutput"+result)
    analysis.outputResult(output2, 1, "outputdata/DataFilterGowallaUserandItemMorethan10")
  }
}
/**
  * 按照地点checkin数目过滤后输出
  */
object DataFilterGowallaItemOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
        "_c0","_c4","_c2","_c3","csv",1),"_2",">",10).toDF("itemid","count")
    val data = analysis.userItemlalonAnalysis("Gowalla/Gowalla_totalCheckins.txt",
      "_c0","_c4","_c2","_c3","csv",1).toDF("userid","itemid","la","lon")
    val output = data.join(filter,"itemid").toDF("_1","_2","_3","_4","_5")
    analysis.outputResult(output,1,"outputdata/GowallaItemCheckinMoreThan10")
  }
}

/**----------------------------------------------------------------------------------
  * 使用测试数据
  */
object YelpUserItemRatingTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val result = analysis.userItemRateAnalysis("input/useritemrating.json",
      "user_id","business_id","stars","json",1)
    result.show()
    analysis.outputResult(result, 1, "output/YelpUserItemRatingAll")
  }
}

object YelpUserFriendTrustTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val result = analysis.userandFriendTrustAnalysis("input/userfriends.json",
      "user_id","friends", 1)
    result.show()
    analysis.outputResult(result, 1, "output/DataAnalysisYelpUserItemTrust")
  }

}

object YelpOneFilterTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val filter = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysis("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_2",">",3).toDF("itemid","count")
    filter.show()
    val data = analysis.userItemRateAnalysis("input/useritemrating.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    data.show()
    //inner join
    val output = data.join(filter,"itemid").toDF("_1","_2","_3","_4")
    output.show()
    analysis.outputResult(output, 1, "output/YelpUserCheckinMorethan10")
  }
}

object YelpTwoFilterTest1{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_1",">",3).toDF("userid","count")
    filter1.show()
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_2",">",1).toDF("itemid","count")
    filter2.show()
    val data = analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    data.show()
    //inner join
    val output1 = data.join(filter1,"userid").join(filter2,"itemid").toDF("_1","_2","_3","_4","_5")
    output1.show()
    val output2 = analysis.transformId(output1,"_2","_1","_3")
    output2.show()
    val output3 = analysis.getAvg(output2,"_1","_2","_3")
    output3.show()
    val result = analysis.analyseSparsity(output2)
    println("DataFilterYelpUserandItemOutput"+result)
    analysis.outputResult(output2, 1, "output/1YelpUserandItemCheckinMorethan10")
  }
}

object YelpTwoFilterTest2{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_1",">",3).toDF("userid","count")
    filter1.show()
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_2",">",1).toDF("itemid","count")
    filter2.show()
    val data = analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    data.show()
    val output1 = data.join(filter1,"userid")
    val output2 = filter2.join(output1,"itemid").toDF("_1","_2","_3","_4","_5")
    output2.show()
    val output3 = analysis.transformId(output2,"_3","_1","_4")
    output3.show()
    val result = analysis.analyseSparsity(output3)
    println("DataFilterYelpUserandItemOutput"+result)
    analysis.outputResult(output3, 1, "output/2YelpUserandItemCheckinMorethan10")
  }
}

object YelpTwoFilterRatingandTrustOutputTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_1",">",1).toDF("userid","count")
    filter1.show()
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
        "user_id","business_id","stars","json",1),"_2",">",1).toDF("itemid","count")
    filter2.show()
    val data = analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    data.show()
    val output1 = data.join(filter1,"userid").join(filter2,"itemid").toDF("_1","_2","_3","_4","_5")
    output1.show()
    val friends = analysis.userandFriendTrustAnalysis("input/userfriends.json",
      "user_id","friends", 1)
    friends.show()
//    val indexrating = analysis.getAvg(analysis.transformId(output1,"_2","_1","_3")
//      ,"_1","_2","_3")
    val dropre = analysis.getAvg(output1.select("_2","_1","_3").toDF("_1","_2","_3"),"_1","_2","_3")
    dropre.show()
    val indexrating = analysis.transformId(dropre,"_1","_2","_3")
    val users = dropre.select("_1").groupBy("_1").count()
    users.show()
    val filteruser = friends.join(users.select("_1"),"_1")
      .join(users.select("_1").toDF("_2"),"_2")
    filteruser.show()
    val indextrust = analysis.transformIdUsingIndexer(output1.select("_2").toDF("_1"), "_1", filteruser)
    indexrating.show()
    indextrust.show()
    analysis.outputResult(indexrating, 1, "output/YelpTwoFilterRatingandTrustOutputTest1")
    analysis.outputResult(indextrust, 1, "output/YelpTwoFilterRatingandTrustOutputTest2")
  }
}

object YelpRatingandTrustOutputTest{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("./src/data/")
    val rating = analysis.userItemRateAnalysisNotrans("input/useritemrating.json",
      "user_id","business_id","stars","json",1)
    rating.show()
    val friends = analysis.userandFriendTrustAnalysis("input/userfriends.json",
      "user_id","friends", 1)
    friends.show()
    val indexrating = analysis.getAvg(analysis.transformId(rating,"_1","_2","_3")
      ,"_1","_2","_3")
    val indextrust = analysis.transformIdUsingIndexer(rating, "_1", friends)
    indexrating.show()
    indextrust.show()
    analysis.outputResult(indexrating, 1, "output/DataAnalysisYelpUserItemTrust1")
    analysis.outputResult(indextrust, 1, "output/DataAnalysisYelpUserItemTrust2")
  }
}
//-------------------------------------------------------------------------------------------

/**
  * 输出所有的Yelp用户商家评分及用户社交网络（trust=1）
  * 注意：将StringID转换为IntegerID时，应该最后转换，中间应使用唯一标识的原始StringID计算。
  * 防止在中途出现ID无法对应的情况。例如，在计算评分表和社交表的时候，
  * 两表的用户ID数量不同，不能先转换再join，应该先join最后再transformIdUsingIndexer
  */
object YelpRatingandTrustOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val rating = analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1)
    rating.show()
    val friends = analysis.userandFriendTrustAnalysis("textdata/yelp_academic_dataset_user.json",
      "user_id","friends", 1)
    friends.show()
    //将userid和itemid重复的记录计算平均分
    val indexrating = analysis.getAvg(analysis.transformId(rating,"_1","_2","_3")
      ,"_1","_2","_3")
    val indextrust = analysis.transformIdUsingIndexer(rating, "_1", friends)
    indexrating.show()
    indextrust.show()
    analysis.outputResult(indexrating, 1, "output/DataAnalysisYelpUserItemAll")
    analysis.outputResult(indextrust, 1, "output/DataAnalysisYelpUserTrust")
  }
}

/**
  * 输出按两个条件过滤的用户商家评分及社交网络
  */
object YelpTwoFilterRatingandTrustOutput{
  def main(args: Array[String]) {
    val analysis = new DataAnalysis("/home/pi/doc/dataset/")
    val filter1 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_1",">",10).toDF("userid","count")
    val filter2 = analysis.userItemRateFilterAnalysis(
      analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
        "user_id","business_id","stars","json",1),"_2",">",10).toDF("itemid","count")
    val data = analysis.userItemRateAnalysisNotrans("textdata/yelp_academic_dataset_review.json",
      "user_id","business_id","stars","json",1).toDF("userid","itemid","starts")
    val output1 = data.join(filter1,"userid").join(filter2,"itemid").toDF("_1","_2","_3","_4","_5")
    val friends = analysis.userandFriendTrustAnalysis("textdata/yelp_academic_dataset_user.json",
      "user_id","friends", 1)
    val dropre = analysis.getAvg(output1.select("_2","_1","_3").toDF("_1","_2","_3"),"_1","_2","_3")
    dropre.show()
    val indexrating = analysis.transformId(dropre,"_1","_2","_3")
    val users = dropre.select("_1").groupBy("_1").count()
    users.show()
    val filteruser = friends.join(users.select("_1"),"_1")
      .join(users.select("_1").toDF("_2"),"_2")
    val indextrust = analysis.transformIdUsingIndexer(output1.select("_2").toDF("_1"), "_1", filteruser)

    analysis.outputResult(indexrating, 1, "output/YelpTwoFilterUserandItemMoretan10Rating")
    analysis.outputResult(indextrust, 1, "output/YelpTwoFilterUserandItemMoretan10Trust")
  }
}

object Testall {
  def main(args: Array[String]) {
    DataAnalysisYelpTrustOutput.main(args)
    DataFilterGowallaUserandItemOutput.main(args)
  }
}