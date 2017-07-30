package scala.poi.datatool

import java.io.{File, PrintWriter}


/**
  * Created by pi on 17-7-30.
  */
class WriteFile {

  def write(path:String,s:String)={
    val writer = new PrintWriter(new File(path))

    writer.write(s)
    writer.close()
  }

}
