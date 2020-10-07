package net.sansa_stack.ml.spark.kernel

import org.apache.jena.graph.Triple
import org.apache.spark.ml.feature.{ CountVectorizer, CountVectorizerModel }
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._


object Uri2Index {
  /*
  * Object to store indices of URIs and labels.
  * */
  var uri2int: Map[String, Int] = Map.empty[String, Int]
  var int2uri: Map[Int, String] = Map.empty[Int, String]
  var uri2label: Map[String, Double] = Map.empty[String, Double]
  var label2uri: Map[Double, String] = Map.empty[Double, String]

  var instanceLabelPair: Map[Int, Double] = Map.empty[Int, Double]

  /*
  * To set the pair of instance and label
  * */
  def setInstanceAndLabel(instanceUri: String, labelUri: String): Unit = {
    var label: Double = 0.0
    if (uri2label.keys.exists(_.equals(labelUri))) {
      label = uri2label(labelUri)
    } else {
      label = (uri2label.size).toDouble
      uri2label += (labelUri -> label)
      label2uri += (label -> labelUri)
    }

    val index: Int = getUriIndexOrSet(instanceUri)

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
  * To get/set URI Index
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
  * To get/set URIs' indices of a tirple
  * */
  def getUriIndexOrSetT(triple: (String, String, String)): (Int, Int, Int) = {
    val index1: Int = getUriIndexOrSet(triple._1)
    val index2: Int = getUriIndexOrSet(triple._2)
    val index3: Int = getUriIndexOrSet(triple._3)

    (index1, index2, index3)
  }
}

class RDFFastTreeGraphKernel(
  @transient val sparkSession: SparkSession,
  val tripleRDD: RDD[Triple],
  val instanceDF: DataFrame,
  val maxDepth: Int) extends Serializable {
  /*
  * Construct Triples DataFrame and Instances DataFrame
  * Also, Get/Set Index for each URI and Literal
  * */
  // Transform URIs to Integers: this works only in Scala.Iterable
  val tripleIntIterable: Iterable[(Int, Int, Int)] = tripleRDD.toLocalIterator.toIterable.map(f =>
    Uri2Index.getUriIndexOrSetT(f.getSubject.toString, f.getPredicate.toString, f.getObject.toString))

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

    tripleIntDF.cache()
    instanceDF.createOrReplaceTempView("instances")
    tripleIntDF.createOrReplaceTempView("triples")

    // Generate Paths for each instance
    var pathDF = sqlContext.sql("SELECT instance, label, '' as path, instance as o FROM instances")
    pathDF.createOrReplaceTempView("df")

    for (i <- 1 to maxDepth) {
      // TODO: break the loop when there are no further new paths
      val intermediateDF = sqlContext.sql(
        "SELECT instance, label, CONCAT(df.path, ',', t.p, ',', t.o) AS path, t.o " +
          "FROM df LEFT JOIN triples t " +
          "WHERE df.o = t.s")

      pathDF = pathDF.union(intermediateDF)
      intermediateDF.createOrReplaceTempView("df")
    }

    // Aggregate paths (Strings to Array[String])
    val aggDF = pathDF.drop("o").orderBy("instance").groupBy("instance", "label").agg(collect_list("path") as "paths")

    // CountVectorize the aggregated paths
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("paths").setOutputCol("features").fit(aggDF)
    val dataML = cvModel.transform(aggDF)

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

    dataForML
  }

  def getMLLibLabeledPoints: RDD[LabeledPoint] = {
    val dataML: DataFrame = MLUtils.convertVectorColumnsFromML(computeFeatures().drop("instance").drop("paths"), "features")

    val dataForMLLib = dataML.rdd.map { f =>
      val label = f.getDouble(0)
      val features = f.getAs[SparseVector](1)
      LabeledPoint(label, features)
    }

    dataForMLLib
  }

}

object RDFFastTreeGraphKernel {

  def apply(
    sparkSession: SparkSession,
    tripleRDD: RDD[Triple],
    instanceDF: DataFrame,
    maxDepth: Int): RDFFastTreeGraphKernel = {

    new RDFFastTreeGraphKernel(sparkSession, tripleRDD, instanceDF, maxDepth)
  }

}
