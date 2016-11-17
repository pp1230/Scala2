package scala

/**
  * Created by pi on 16-11-15.
  */
class VectorSlicerEx {

}

import java.util.Arrays

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


object VectorSlicerEx {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
      .appName("VectorSlicerExample")
      .master("local[*]")
      .getOrCreate()

    // $example on$
    //val data = Arrays.asList(Row(Vectors.dense(-1.0, 2.3, 3.0)))
    //Vector格式（向量維數，向量索引，向量值value值）
    val data = Arrays.asList(Row(Vectors.sparse(5, Array(1, 2, 3, 4), Array(7, 8, 9, 6))))
    println(data)

    val defaultAttr = NumericAttribute.defaultAttr
//    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
//    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])
//
//    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))

    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    //设使用下標設置留下的部分
    slicer.setIndices(Array(2))

    println("----------output1----------")
    val output1 = slicer.transform(dataset)
    output1.show()

//    println("----------output2----------")
//    //使用名稱增加設置
//    slicer.setNames(Array("f3"))
//    val output2 = slicer.transform(dataset)
//    output2.show()
    // $example off$


    spark.stop()
  }
}

