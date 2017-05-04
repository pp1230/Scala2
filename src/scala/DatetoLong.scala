package scala

import java.sql.Timestamp


/**
  * Created by pi on 17-4-28.
  */
class DatetoLong {

}

object DatetoLong{
  def main(args: Array[String]) {
    val str = "2012-08-01"+" 00:00:00"
    val time = Timestamp.valueOf(str).getTime
    println(time)
  }
}