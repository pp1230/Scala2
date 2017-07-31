package scala.poi.datatool

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date


/**
  * Created by pi on 17-7-30.
  */
class WriteFile {

  def write(path:String,file:String,s:String)={
    val writer = new PrintWriter(new File(path+file+getNowDate()))

    writer.write(s)
    writer.close()
  }

  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var hehe = dateFormat.format( now )
    hehe
  }

}
