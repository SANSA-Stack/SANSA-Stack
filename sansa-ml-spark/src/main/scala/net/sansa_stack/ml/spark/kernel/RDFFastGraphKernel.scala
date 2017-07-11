package net.sansa_stack.ml.spark.kernel

import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.jena.graph.Node
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.types.{StructField, StructType, IntegerType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

class RDFFastGraphKernel (@transient val sparkSession: SparkSession,
                          val tripleRDD: TripleRDD,
                          val maxDepth: Int) extends Serializable {

  var uri2int:Map[String, Int] = Map.empty[String, Int]
  var int2uri: Map[Int, String] = Map.empty[Int, String]
  def getUriIndexOrSet (uri: String) : Int = {
    var index : Int = 0
    if (uri2int.keys.exists(_ == uri)) {
      index = uri2int(uri)
    } else {
      index = uri2int.size + 1
      uri2int += (uri -> index)
      int2uri += (index -> uri)
    }
    index
  }

  var path2index: Map[List[Int], Int] = Map.empty[List[Int], Int]
  var index2path: Map[Int, List[Int]] = Map.empty[Int, List[Int]]
  def getPathIndexOrSet (path: List[Int]) : Int = {
    var index : Int = 0
    if (path2index.keys.exists(_ == path)) {
      index = path2index(path)
    } else {
      index = path2index.size + 1
      path2index += (path -> index)
      index2path += (index -> path)
    }
    index
  }

  val tripleIntIterable: Iterable[(Int, Int, Int)] = tripleRDD.getTriples.map(f =>
    (getUriIndexOrSet(f.getSubject.toString), getUriIndexOrSet(f.getPredicate.toString), getUriIndexOrSet(f.getObject.toString)))

  val tripleIntRDD: RDD[Row] =  sparkSession.sparkContext.parallelize(tripleIntIterable.toList).map(f => Row(f._1, f._2, f._3))
  val tripleStruct: StructType = new StructType(Array(StructField("s", IntegerType), StructField("p", IntegerType), StructField("o", IntegerType) ))
  val tripleDF: DataFrame = sparkSession.createDataFrame(tripleIntRDD, tripleStruct);

  val instanceDF: DataFrame = tripleDF.select("s").distinct().toDF("instance")

  def compute(): Unit = {
    val sqlContext = sparkSession.sqlContext

    instanceDF.createOrReplaceTempView("instances")
    tripleDF.createOrReplaceTempView("triples")

    var df = sqlContext.sql(
      "select i.instance as instance, CONCAT(t.p, ',', t.o) as path, t.o " +
        "from instances i left join triples t " +
        "where i.instance = t.s")
    df.createOrReplaceTempView("df")

    for (i <- 2 to maxDepth) {
      val intermediateDF = sqlContext.sql(
        "select instance, CONCAT(df.path, ',', t.p, ',', t.o) as path, t.o " +
          "from df df left join triples t " +
          "where df.o = t.s")
      df = df.union(intermediateDF)
      intermediateDF.createOrReplaceTempView("df")
    }

    df.show(1000)

  }

}


object RDFFastGraphKernel {

  def apply(sparkSession: SparkSession,
            tripleRDD: TripleRDD,
            maxDepth: Int): RDFFastGraphKernel = new RDFFastGraphKernel(sparkSession, tripleRDD, maxDepth)

}
