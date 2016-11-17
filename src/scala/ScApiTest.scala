package scala

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by pi on 16-8-9.
  */
class ScApiTest {
  var conf = new SparkConf().setAppName("Scapi").setMaster("local[*]")
  //var conf = new SparkConf().setAppName("Scapi").setMaster("spark://192.168.1.104:4050")
  var sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  //sc.addJar("/home/pi/doc/WORKSPACE/github_demo/alicloud-ams-demo/Scala2/out/artifacts/Scala2_jar/Scala2.jar")
}

object ScApiTest{
  def main(args: Array[String]) {
    val sc = new ScApiTest().sc
    val data = Array(1,2,3)
    //生成rdd
    val rdd1 = sc.parallelize(data,1)
    rdd1.foreach(i => println(i))
    //按一定间隔生成rdd
    val rdd2 = sc.range(1,10,3,1)
    rdd2.foreach(i => println(i))

    //PairRDDFunctions
    val rdd4 = sc.parallelize(Array(("hello",1),("pi",1)))
    val data1 = "hello world hello pi pi haha"
    //统计每个单词个数
    val rdd3 = sc.parallelize(data1.split(" "))
      .map(i => (i,1)).reduceByKey(_+_)

    //合并相同单词
    val rdd5 = rdd3.join[Int](rdd4)

    rdd3.foreach(i => println(i))
    rdd5.foreach(i => println(i))


    /**
      * 运行结果1
      * (haha,1)
      * (hello,2)
      * (world,1)
      * (pi,2)
      *
      * 运行结果2
      * (pi,(2,1))
      * (hello,(2,1))
      */

    //Transformations
      println("============="+"Transformations"+"============")

    val numData = Array(1,2,2,3)
    val stringArray = Array("pi","huaiyu","hello","pi")
    val stringData = "pi huai yu hello pi"
    val stringArrayData = Array("hello world pi","pi huai yu")

    val numRdd = sc.parallelize(numData)
    val stringRdd = sc.parallelize(stringData)
    /**
      * map(func)
      * Return a new distributed dataset formed by
      * passing each element of the source through a function func.
      */
    println("---------"+"map"+"----------")
    val mapRdd = numRdd.map(_+1)
    mapRdd.foreach(println(_))

    /**
      * filter(func)
      * Return a new dataset formed by selecting those
      * elements of the source on which func returns true.
      */
    println("---------"+"filter"+"----------")
    val filterRdd = numRdd.filter(i => i<2)
    filterRdd.foreach(println(_))

    /**
      * flatMap(func)
      * Similar to map, but each input item can be
      * mapped to 0 or more output items (so func should return a Seq rather than a single item).
      * flatmap和map的不同在于，flatmap从m映射出n个平行元素，map从n映射出相等n个元素
      */
    println("---------"+"flatMap"+"----------")
    val flatMapRdd1 = sc.parallelize(stringArrayData).flatMap(_.split(" "))
    flatMapRdd1.foreach(println(_))
    println("---------"+"map"+"----------")
    val flatMapRdd2 = sc.parallelize(stringArrayData).map(_.split(" "))
    flatMapRdd2.foreach(i => {
      println("item")
      i.foreach(println(_))
    }
    )

    /**
      * mapPartitions(func)
      * 该函数和map函数类似，只不过映射函数的参数由RDD中的每一个元素变成了RDD中每一个分区的迭代器。
      * 如果在映射的过程中需要频繁创建额外的对象，使用mapPartitions要比map高效的多。
      * 参数preservesPartitioning表示是否保留父RDD的partitioner分区信息。
      */
    println("---------"+"mapPartitions"+"----------")
    val mapPartitionsRdd = sc.parallelize(stringArrayData).mapPartitions(_.map(_.split(" ")))
    mapPartitionsRdd.foreach(_.foreach(println(_)))

    //val rdd6 = sc.makeRDD(Array(3,5,8),3)
    val rdd6 = sc.parallelize(Vector(0,(1,2,3),(4,5,6)))
    println(rdd6)
    val rdd7 = rdd6.mapPartitions {
      x => {

        var result = List[String]()
        var i = ""
        var j = ""
        while (x.hasNext) {
          j = x.next().toString
          i = j
          println("---------->"+i+"/"+j)
        }
        result.::(i).iterator

      }
    }
    println("---------->1"+rdd7.first())
    rdd7.foreach(r => println("---------->2"+r))
    println("---------->3"+rdd7.collect().apply(2))

    val rdd8 = rdd6.mapPartitionsWithIndex{
      (index,x) =>{

        var result = List[String]()
        var i = ""
        var j = ""
        while (x.hasNext) {
          j = x.next().toString
          i = j
          println("---------->"+i+"/"+j)
        }
        result.::(index).::(i).iterator
      }
    }
    rdd8.foreach(r => println("---------->4"+r))
    /**
      * mapPartitionsWithIndex(func)
      * Similar to mapPartitions, but also provides func with an integer value representing the index of the partition,
      * so func must be of type (Int, Iterator<T>) => Iterator<U> when running on an RDD of type T.
      */
    println("---------"+"mapPartitionsWithIndex"+"----------")
    val mapPartitionsWithIndexRdd = sc.parallelize(stringArrayData,2).mapPartitionsWithIndex((a,b) =>{
      if(a == 1) {
        println(a)
        b.map(_.split(""))
      }
      else {
        println(a)
        b.map(_.split(" "))
      }
    }
    )
    mapPartitionsWithIndexRdd.foreach(_.foreach(println(_)))

    /**
      * sample(withReplacement, fraction, seed)
      * Sample a fraction fraction of the data, with or without replacement,
      * using a given random number generator seed.
      * 对RDD中的集合内元素进行采样，第一个参数withReplacement是true表示有放回取样，false表示无放回。
      * 第二个参数表示比例，第三个参数是随机种子。
      */
    println("---------"+"sample"+"----------")
    val sampleRdd = numRdd.sample(false,0.5)
    sampleRdd.foreach(println(_))

    /**
      *   union(otherDataset)
      * 	Return a new dataset that contains the union of
      * 	the elements in the source dataset and the argument.
      */

    println("---------"+"union"+"----------")
    val unionRdd = numRdd.union(sampleRdd)
    unionRdd.foreach(println(_))

    /**
      * intersection(otherDataset)
      * Return a new RDD that contains the intersection of
      * elements in the source dataset and the argument.
      */
    println("---------"+"intersection"+"----------")
    val intersectionRdd = numRdd.intersection(sampleRdd)
    intersectionRdd.foreach(println(_))

    /**
      * distinct([numTasks]))
      * Return a new dataset that contains the distinct elements of the source dataset.
      */
    println("---------"+"distinct"+"----------")
    val distinctRdd = numRdd.distinct()
    distinctRdd.foreach(println(_))

    /**
      *   groupByKey([numTasks])
      * 	When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.
      * 	Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key,
      * 	using reduceByKey or aggregateByKey will yield much better performance.
      * 	Note: By default, the level of parallelism in the output depends on the number of
      * 	partitions of the parent RDD. You can pass an optional numTasks argument to set a different number of tasks.
      */
    println("---------"+"groupByKey"+"----------")
    val groupByKeyRdd = sc.parallelize(stringArrayData).flatMap(_.split(" ")).map((_,1)).groupByKey()
    groupByKeyRdd.foreach(println(_))

    /**
      *   reduceByKey(func, [numTasks])
      * 	When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs
      * 	where the values for each key are aggregated using the given reduce function func,
      * 	which must be of type (V,V) => V. Like in groupByKey,
      * 	the number of reduce tasks is configurable through an optional second argument.
      */
    println("---------"+"reduceByKey"+"----------")
    val reduceByKeyRdd = sc.parallelize(stringArrayData).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    reduceByKeyRdd.foreach(println(_))

    /**
      * aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])
      * Aggregate the values of each key, using given combine functions and a neutral "zero value".
      * This function can return a different result type, U, than the type of the values in this RDD, V.
      * Thus, we need one operation for merging a V into a U and one operation for merging two U's, as in scala.
      * TraversableOnce. The former operation is used for merging values within a partition,
      * and the latter is used for merging values between partitions. To avoid memory allocation,
      * both of these functions are allowed to modify and return their first argument instead of creating a new U.
      * 聚合键值对的value，通过seqOp方法可以聚合不同类型的value。例如(K,V)，zerovalue类型为U，通过seqOp方法返回U类型的值。
      * 最终通过comOp方法将所有value聚合。
      */
    println("---------"+"aggregateByKey"+"----------")
    val aggregateByKeyRdd = sc.parallelize(stringArrayData).flatMap(_.split(" "))
      .map((_,"a")).aggregateByKey(1)((a,b) => a,_+_)
    aggregateByKeyRdd.foreach(println(_))

    /**
      * sortByKey([ascending], [numTasks])
      * When called on a dataset of (K, V) pairs where K implements Ordered,
      * returns a dataset of (K, V) pairs sorted by keys in ascending or descending order,
      * as specified in the boolean ascending argument.
      */
    println("---------"+"sortByKey"+"----------")
    val sortByKeyRdd = sc.parallelize(stringArrayData,1).flatMap(_.split(" "))
      .map((_,"a")).sortByKey()
    sortByKeyRdd.foreach(println(_))

    /**
      * join(otherDataset, [numTasks])
      * When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs
      * with all pairs of elements for each key. Outer joins are supported through leftOuterJoin,
      * rightOuterJoin, and fullOuterJoin.
      */
    println("---------"+"join"+"----------")
    val stringDataTestJoin = "pi huai yu"
    val joinRdd1 = sc.parallelize(stringArrayData).flatMap(_.split(" "))
      .map((_,"a"))
    val joinRdd2 = sc.parallelize(stringDataTestJoin.split(" ")).map((_,1))
    val joinRdd3 = joinRdd1.join(joinRdd2)
    joinRdd3.foreach(println(_))

    /**
      *   cogroup(otherDataset, [numTasks])
      * 	When called on datasets of type (K, V) and (K, W), returns a dataset of
      * 	(K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called groupWith.
      *   相比于join，cogroup会将Key相同的元素全部聚合
      */

    println("---------"+"cogroup"+"----------")
    val cogroupRdd = joinRdd1.cogroup(joinRdd2)
    cogroupRdd.foreach(println(_))

    /**
      * cartesian(otherDataset)
      * When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).
      */
    println("---------"+"cartesian"+"----------")
    val cartesianRdd = stringRdd.cartesian(numRdd)
    cartesianRdd.foreach(println(_))

    /**
      * pipe(command, [envVars])
      * Pipe each partition of the RDD through a shell command,
      * e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to
      * its stdout are returned as an RDD of strings.
      * 如何使用？
      */

    /**
      * coalesce(numPartitions)
      * Decrease the number of partitions in the RDD to numPartitions.
      * Useful for running operations more efficiently after filtering down a large dataset.
      */
    println("---------"+"coalesce"+"----------")
    val coalesceRdd = numRdd.filter(i => i<2).coalesce(1)
    coalesceRdd.foreach(println(_))

    /**
      *   repartition(numPartitions)
      * 	Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them.
      * 	This always shuffles all data over the network.
      */
    println("---------"+"repartition"+"----------")
    val repatitionRdd = numRdd.repartition(2)
    repatitionRdd.foreach(println(_))

    /**
      * repartitionAndSortWithinPartitions(partitioner)
      * Repartition the RDD according to the given partitioner and, within each resulting partition,
      * sort records by their keys. This is more efficient than calling repartition and then
      * sorting within each partition because it can push the sorting down into the shuffle machinery.
      * api中没看见？
      */

    //Actions

    println("============="+"Actions"+"============")
    println("---------"+"reduce"+"----------")
    val reduceArray = numRdd.reduce(_+_)
    println(reduceArray)

    println("---------"+"count"+"----------")
    val count = numRdd.count()
    println(count)

    println("---------"+"first"+"----------")
    println(numRdd.first())

    println("---------"+"take"+"----------")
    numRdd.take(2).foreach(println(_))

    /**
      * takeSample(withReplacement, num, [seed])
      * Return an array with a random sample of num elements of the dataset,
      * with or without replacement, optionally pre-specifying a random number generator seed.
      */

    println("---------"+"takeSample"+"----------")
    numRdd.takeSample(false,2).foreach(println(_))

    /**
      * takeOrdered(n, [ordering])
      * Return the first n elements of the RDD using either their natural order or a custom comparator.
      */
    println("---------"+"takeOrdered"+"----------")
    numRdd.takeOrdered(2).foreach(println(_))

//    println("---------"+"saveAsTextFile"+"----------")
//    numRdd.saveAsTextFile("/home/pi/doc/sparkout/TextFile")
//
//    println("---------"+"saveAsObjectFile"+"----------")
//    numRdd.saveAsObjectFile("/home/pi/doc/sparkout/ObjectFile")
//
//    println("---------"+"saveAsSequenceFile"+"----------")
//    sc.parallelize(stringData.split(" ")).map((_,1)).saveAsSequenceFile("/home/pi/doc/sparkout/SequenceFile")

    println("---------"+"countByKey"+"----------")
    sc.parallelize(stringData.split(" ")).map((_,1)).countByKey().foreach(println(_))

  }


}

