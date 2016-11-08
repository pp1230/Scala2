package scala

/**
  * Created by pi on 16-8-8.
  * Hello World!
  */
class Hello {

}

object Hello {
  def main(args: Array[String]) {
    println("Hello World")

    var a = 2.0
    var b = 2.0
    var c = 0.0
    val alpha = 0.01
    for(i<- 1 to 5000){
      println("num:"+i)
      a = a - alpha*(10*b + 30*a -29)
      b = b - alpha*(4*b + 10*a -10)
      c = b*b + 20*a*b - 2*b -20*a + 30
      println("a:"+a+"  "+"b:"+b+"  "+"c:"+c)
    }
  }
}
