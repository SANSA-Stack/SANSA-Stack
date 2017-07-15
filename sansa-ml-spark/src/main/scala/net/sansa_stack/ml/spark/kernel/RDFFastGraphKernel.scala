package net.sansa_stack.ml.spark.kernel

import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.jena.graph.Node
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.types.{StructField, StructType, IntegerType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

import org.apache.spark.sql.functions._
//import org.apache.spark.sql.Dataset
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

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
    println("Aggregate paths")
    //o is not needed anymore
    val df2 = df.drop("o")
    //aggregate paths (Strings to Array[String])
    val aggdf = df2.orderBy("instance").groupBy("instance").agg(collect_list("path") as "paths")
    aggdf.show(1000)
    println("compute cvm")
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("paths").setOutputCol("index").fit(aggdf)
    val cvTrans = cvModel.transform(aggdf)
    // Output can be large, use false to show the whole columns
    // Note: The paths in the output are not ordered
    cvTrans.show(false)
    
    // Alternatively print each line
    println("Print foreach")
    cvTrans.foreach(println(_))
  }

}


object RDFFastGraphKernel {

  def apply(sparkSession: SparkSession,
            tripleRDD: TripleRDD,
            maxDepth: Int): RDFFastGraphKernel = new RDFFastGraphKernel(sparkSession, tripleRDD, maxDepth)

}