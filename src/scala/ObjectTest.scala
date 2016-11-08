package scala

/**
  * Created by pi on 16-8-8.
  * class和object的区别
  */

/**
  * scala没有静态的修饰符，但object下的成员都是静态的 ,
  * 若有同名的class,这其作为它的伴生类。
  * 在object中一般可以为伴生类做一些初始化等操作
  */

class ObjectTest {

  var data1 = Array(1,2,3)
  def test1()={
    println(data1)
  }
}

object ObjectTest {

  var data2 = Array(1,2)
  //私有成员不能在其他object中访问
  private var data3 = "pipi"

  def test2() = {
    new ObjectTest()
  }
}

object PrintObject {
  def main(args: Array[String]) {

    var a = ObjectTest.test2()
    println(a.data1.length)
    println(ObjectTest.data2.length)
  }
}
