package scala

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by pi on 17-5-21.
  */
class WordCount {

}

object WordCount{
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    var sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

   // val rddinput = sc.textFile("hdfs://139.196.27.14:9100/homework/input/input.txt")
    val textinput = sc.textFile("hdfs://139.196.27.14:9100/homework/input/input.txt").collect()
    //textinput.foreach(println(_))

//    var rdd = sc.parallelize("".split("")).map((_,0))
//    val rdd1 = rddinput.map(r => r.replaceAll("[\\pP\\p{Punct}\\d]", "")
//      .replace("  "," ").toLowerCase.split(" ").map(r => (r,1)))
//    rdd1.foreach(r => println(r(0)))


//    var result = sc.parallelize("12the, the:".replaceAll("[\\pP\\p{Punct}\\d]", "").split(" "))
//      .map((_,0)).reduceByKey(_ + _).map(r => ("("+r._1+","+r._1+")",r._2))
//      //.foreach(print(_))
    var result = sc.parallelize("".split("")).map((_,0))

    for (i <- 0 to textinput.size - 1) {
      val rdd1 = sc.parallelize(textinput(i).replaceAll("[\\pP\\p{Punct}\\d]", "")
        .replace("  "," ").toLowerCase.split(" ")).map((_, 1)).reduceByKey(_ + _).filter(!_._1.equals(""))

      val rdd2 = rdd1.map(_._1)
      val rdd3 = rdd2.cartesian(rdd2)
      val rdd4 = rdd3.join(rdd1).map(r => {
        if(r._2._1.equals(r._1))
          if(r._2._2 >= 2)
            ("("+r._2._1+","+ r._1+")", 1)
        else ("",0)
        else
        ("("+r._2._1+","+ r._1+")", r._2._2)
      })

      //println("------------count line "+i+"---------------")
      //rdd4.foreach(print(_))

      result = result.++(rdd4)
      //result.foreach(println(_))
    }
    result = result.filter(!_._1.equals("")).reduceByKey(_+_).sortByKey()
    //result.foreach(println(_))
    result.saveAsTextFile("hdfs://139.196.27.14:9100/output/wordcount"+TimeStamp.getCurrentTime+".txt")
  }
}
