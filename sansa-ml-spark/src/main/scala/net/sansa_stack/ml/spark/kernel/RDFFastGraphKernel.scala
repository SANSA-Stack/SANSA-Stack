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

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint

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
  // property should be an additional input to apply(...)
  val property: String = "http://xmlns.com/foaf/0.1/hasTopic"
  
  // Same as before, but operating on Triples
  var uri2intT:Map[String, Int] = Map.empty[String, Int]
  var int2uriT: Map[Int, String] = Map.empty[Int, String]
  // Map uri(subject) to a property class
  var uri2prop : Map[Int, Int] = Map.empty[Int, Int]
  def getUriIndexOrSetT (triple: (String, String, String)) : (Int,Int,Int) = {
    var index1 : Int = 0
    var index2 : Int = 0
    var index3 : Int = 0
    // Check subject
    if (uri2intT.keys.exists(_ == triple._1)) {
      index1 = uri2intT(triple._1)
    } else {
      index1 = uri2intT.size + 1
      uri2intT += (triple._1 -> index1)
      int2uriT += (index1 -> triple._1)
    }
    // Check object
    if (uri2intT.keys.exists(_ == triple._3)) {
      index3 = uri2intT(triple._3)
    } else {
      index3 = uri2intT.size + 1
      uri2intT += (triple._3 -> index3)
      int2uriT += (index3 -> triple._3)
    }
    // Check predicate
    //println(triple._2)
    if (property == triple._2) {
      // In case of desired property we add a class to our map
      uri2prop += (index1 -> index3)
      // And return (-1,-1,-1)
      // Filtering will remove those lines later on to prevent SVM-fitting only based on the property itself
      index1 = -1
      index2 = -1
      index3 = -1
    }
    else if (uri2intT.keys.exists(_ == triple._2)) {
      index2 = uri2intT(triple._2)
    } else {
      index2 = uri2intT.size + 1
      uri2intT += (triple._2 -> index2)
      int2uriT += (index2 -> triple._2)
    }
    (index1, index2, index3)
  }
  
  // Use triple-version instead of seperate one
  //val tripleIntIterable: Iterable[(Int, Int, Int)] = tripleRDD.getTriples.map(f =>
    //(getUriIndexOrSet(f.getSubject.toString), getUriIndexOrSet(f.getPredicate.toString), getUriIndexOrSet(f.getObject.toString)))
  val tripleIntIterable: Iterable[(Int, Int, Int)] = tripleRDD.getTriples.map(f =>
    getUriIndexOrSetT(f.getSubject.toString,f.getPredicate.toString,f.getObject.toString))  
  
  // Apply filtering to remove the (-1,-1,-1) rows
  //val tripleIntRDD: RDD[Row] =  sparkSession.sparkContext.parallelize(tripleIntIterable.toList).map(f => Row(f._1, f._2, f._3))
  val tripleIntRDD: RDD[Row] =  sparkSession.sparkContext.parallelize(tripleIntIterable.toList).filter(f => (f._1>=0)).map(f => Row(f._1, f._2, f._3))
  
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
    
    // Show uri(subject)-to-property map
    uri2prop.foreach(println(_))
    
    // Alternatively print each line
    //println("Print foreach")
    //cvTrans.foreach(println(_))
    
    // Convert to the correct SparseVector and drop paths
    val dataML = MLUtils.convertVectorColumnsFromML(cvTrans.drop("paths"), "index")
    dataML.foreach(println(_))
    // Map to RDD[LabeledPoint] for SVM-support
    // Dropping unlabeled data might be useful ( .filter(label>=0) )
    val data = dataML.rdd.map{f =>
      val label = uri2prop.getOrElse(f.getInt(0), -1.toInt).toDouble
      val features = f.getAs[org.apache.spark.mllib.linalg.SparseVector](1)
      LabeledPoint(label, features)
      }
    
    // Some stuff for SVM:
    
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    
    println("data")
    data.foreach(println(_))
    println("training")
    training.foreach(println(_))
    println("test")
    test.foreach(println(_))
    
    //val numIterations = 100
    //val model = SVMWithSGD.train(training, numIterations)
    
  }

}


object RDFFastGraphKernel {

  def apply(sparkSession: SparkSession,
            tripleRDD: TripleRDD,
            maxDepth: Int): RDFFastGraphKernel = new RDFFastGraphKernel(sparkSession, tripleRDD, maxDepth)

}