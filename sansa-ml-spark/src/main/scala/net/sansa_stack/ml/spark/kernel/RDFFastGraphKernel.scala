package net.sansa_stack.ml.spark.kernel

import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

object Uri2Index {
  /*
  * Object to store indices of uris and labels.
  * */
  var uri2int: Map[String, Int] = Map.empty[String, Int]
  var int2uri: Map[Int, String] = Map.empty[Int, String]
  var uri2label: Map[String, Double] = Map.empty[String, Double]
  var label2uri: Map[Double, String] = Map.empty[Double, String]

  var instanceLabelPair: Map[Int, Double] = Map.empty[Int, Double]

  /*
  * To set the pair of instance and label
  * */
  def setInstanceAndLabel(instanceUri:String, labelUri:String): Unit = {
    var label : Double = 0.0
    if (uri2label.keys.exists(_.equals(labelUri))) {
      label = uri2label(labelUri)
    } else {
      label = (uri2label.size).toDouble
      uri2label += (labelUri -> label)
      label2uri += (label -> labelUri)
    }

    val index:Int = getUriIndexOrSet(instanceUri)

    instanceLabelPair += (index -> label)
  }

  /*
  * Get DataFrame of pairs of Instance and Label
  * */
  def getInstanceLabelsDF(sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._
    val instanceLabelsDF: DataFrame = sparkSession.sparkContext
      .parallelize(instanceLabelPair.map(f => (f._1, f._2)).toList)
      .toDF("instance", "label")

    instanceLabelsDF
  }

  /*
  * To get/set Uri Index
  * */
  def getUriIndexOrSet(uri: String): Int = {
    var index: Int = 0
    if (uri2int.keys.exists(_.equals(uri))) {
      index = uri2int(uri)
    } else {
      index = uri2int.size + 1
      uri2int += (uri -> index)
      int2uri += (index -> uri)
    }

    index
  }

  /*
  * To get/set Uris' indices of a tirple
  * */
  def getUriIndexOrSetT(triple: (String, String, String)): (Int, Int, Int) = {
    val index1: Int = getUriIndexOrSet(triple._1)
    val index2: Int = getUriIndexOrSet(triple._2)
    val index3: Int = getUriIndexOrSet(triple._3)

    (index1, index2, index3)
  }
}

// TODO: RENAME file,class,object names -> maybe `RDFIntersectionTreePathKernel`
class RDFFastGraphKernel(@transient val sparkSession: SparkSession,
                         val tripleRDD: TripleRDD,
                         val instanceDF: DataFrame,
                         val maxDepth: Int
                        ) extends Serializable {
  /*
  * Construct Triples DataFrame and Instances DataFrame
  * Also, Get/Set Index for each URI and Literal
  * */
  // Transform URIs to Integers: this works only in Scala.Iterable
  val tripleIntIterable: Iterable[(Int, Int, Int)] = tripleRDD.getTriples.map(f =>
      Uri2Index.getUriIndexOrSetT(f.getSubject.toString,f.getPredicate.toString,f.getObject.toString))

  import sparkSession.implicits._
  val tripleIntDF: DataFrame = sparkSession.sparkContext
    .parallelize(tripleIntIterable.toList)
    .map(f => (f._1, f._2, f._3)).toDF("s", "p", "o")


  def computeFeatures(): DataFrame = {
    /*
    * Return dataframe schema
    * root
      |-- instance: integer (nullable = true)
      |-- label: double (nullable = true)
      |-- paths: array (nullable = true)
      |    |-- element: string (containsNull = true)
      |-- features: vector (nullable = true)
    * */
    val sqlContext = sparkSession.sqlContext

    instanceDF.createOrReplaceTempView("instances")
    tripleIntDF.createOrReplaceTempView("triples")

//    println("Generating Paths from each instance")
    // Generate Paths from each instance
    var pathDF = sqlContext.sql(
      "SELECT i.instance AS instance, i.label AS label, CONCAT(t.p, ',', t.o) AS path, t.o " +
        "FROM instances i LEFT JOIN triples t " +
        "ON i.instance = t.s")
    pathDF.createOrReplaceTempView("df")

    for (i <- 2 to maxDepth) {
      // TODO: break the loop when there's no more new paths
      val intermediateDF = sqlContext.sql(
        "SELECT instance, label, CONCAT(df.path, ',', t.p, ',', t.o) AS path, t.o " +
          "FROM df LEFT JOIN triples t " +
          "ON df.o = t.s")

      pathDF = pathDF.union(intermediateDF)
      intermediateDF.createOrReplaceTempView("df")
    }

//    println("Aggregate paths")
    //aggregate paths (Strings to Array[String])
    val aggDF = pathDF.drop("o").orderBy("instance").groupBy("instance", "label").agg(collect_list("path") as "paths")
//    aggDF.show(100)

//    println("Compute CountVectorizerModel")
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("paths").setOutputCol("features").fit(aggDF)
    val dataML = cvModel.transform(aggDF)

//    dataML.printSchema()
    dataML.show(20)

    dataML
  }
  
  def computeFeatures2(): DataFrame = {
    /*
    * Return dataframe schema
    * root
      |-- instance: integer (nullable = true)
      |-- label: double (nullable = true)
      |-- paths: array (nullable = true)
      |    |-- element: string (containsNull = true)
      |-- features: vector (nullable = true)
    * */
    
    val pathDF: DataFrame = sparkSession.sparkContext
    .parallelize(tripleRDD.getTriples.map(f => (f.getSubject.toString,f.getPredicate.toString,f.getObject.toString)).toList)
    .map(f => if(f._2 != "http://data.bgs.ac.uk/ref/Lexicon/hasTheme") (f._1,-1,f._2+f._3) else (f._1,f._3.hashCode(),"")).toDF("s", "hash_label", "path")

//    println("Aggregate paths")
    //aggregate paths (Strings to Array[String])
    //aggregate hash_labels to maximum per subject and filter unassigned subjects
    val aggDF = pathDF.orderBy("s").groupBy("s").agg(max("hash_label") as "hash_label",collect_list("path") as "paths").filter("hash_label>=0")
    //cast hash_label from int to string to use StringIndexer later on
    .selectExpr("s","cast(hash_label as string) hash_label","paths")
    aggDF.show(100)

//    println("Compute CountVectorizerModel")
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("paths").setOutputCol("features").fit(aggDF)
    val dataML = cvModel.transform(aggDF).drop("paths")

//    dataML.printSchema()
    dataML.show(100)

    val indexer = new StringIndexer()
    .setInputCol("hash_label")
    .setOutputCol("label")
    .fit(dataML)
    val indexed = indexer.transform(dataML).drop("hash_label")
    indexed.show(100)

    dataML
  }

  def getMLFeatureVectors: DataFrame = {
    /*
    * root
      |-- label: double (nullable = true)
      |-- features: vector (nullable = true)
    * */
    val dataML: DataFrame = computeFeatures()
    val dataForML: DataFrame = dataML.drop("instance").drop("paths")
//    Uri2Index.label2uri.foreach(println(_))
    dataForML
  }

  def getMLLibLabeledPoints: RDD[LabeledPoint] = {
    val dataML: DataFrame = MLUtils.convertVectorColumnsFromML(computeFeatures().drop("instance").drop("paths"), "features")

    //  Map to RDD[LabeledPoint] for SVM-support
    val dataForMLLib = dataML.rdd.map { f =>
          val label = f.getDouble(0)
          val features = f.getAs[SparseVector](1)
          LabeledPoint(label, features)
        }
    
    dataForMLLib
  }

}


object RDFFastGraphKernel {

  def apply(sparkSession: SparkSession,
            tripleRDD: TripleRDD,
            instanceDF: DataFrame,
            maxDepth: Int
           ): RDFFastGraphKernel = {

    new RDFFastGraphKernel(sparkSession, tripleRDD, instanceDF, maxDepth)
  }

}