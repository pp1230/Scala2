package scala.poi.datatool

/**
  * Created by pi on 17-7-1.
  */
class test {

}
object test {
  def main (args: Array[String] ) {
    var randomdata = new GetRandomData(
      "/home/pi/Documents/DataSet/dataset/yelp_academic_dataset_review.json",
      "/home/pi/Documents/DataSet/dataset/yelp_randomdata1.csv",
      0.005
    )
    randomdata.getPecentData()
}
}