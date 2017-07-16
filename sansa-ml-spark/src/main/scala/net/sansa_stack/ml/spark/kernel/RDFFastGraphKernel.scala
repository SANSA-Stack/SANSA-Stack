package net.sansa_stack.ml.spark.kernel

import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, IntegerType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint



object Uri2IndexT {
  var uri2int: Map[String, Int] = Map.empty[String, Int]
  var int2uri: Map[Int, String] = Map.empty[Int, String]
  // Map uri(subject) to a property class
  var uri2prop: Map[Int, Int] = Map.empty[Int, Int]
  var propertyToPredict: String = ""

  def getUriIndexOrSetT(triple: (String, String, String)): (Int, Int, Int) = {
    var index1: Int = 0
    var index2: Int = 0
    var index3: Int = 0

    // Check subject
    if (uri2int.keys.exists(_.equals(triple._1))) {
      index1 = uri2int(triple._1)
    } else {
      index1 = uri2int.size + 1
      uri2int += (triple._1 -> index1)
      int2uri += (index1 -> triple._1)
    }

    // Check object
    if (uri2int.keys.exists(_.equals(triple._3))) {
      index3 = uri2int(triple._3)
    } else {
      index3 = uri2int.size + 1
      uri2int += (triple._3 -> index3)
      int2uri += (index3 -> triple._3)
    }

    // Check predicate
    if ( propertyToPredict == triple._2 ) {
      // In case of desired property we add a class to our map
      uri2prop += (index1 -> index3)
      // And return (-1,-1,-1)
      // Filtering will remove those lines later on to prevent SVM-fitting only based on the property itself
      index1 = -1
      index2 = -1
      index3 = -1
    }
    else if (uri2int.keys.exists(_.equals(triple._2))) {
      index2 = uri2int(triple._2)
    } else {
      index2 = uri2int.size + 1
      uri2int += (triple._2 -> index2)
      int2uri += (index2 -> triple._2)
    }

    (index1, index2, index3)
  }
}


class RDFFastGraphKernel(@transient val sparkSession: SparkSession,
                         val tripleRDD: TripleRDD,
                         val maxDepth: Int,
                         val propertyToPredict: String
                        ) extends Serializable {
  /*
  * Construct Triples DataFrame and Instances DataFrame
  * Also, Get/Set Index for each URI and Literal
  * */
  // Transform URIs to Integers: this works only in Scala.Iterable
  Uri2IndexT.propertyToPredict = propertyToPredict
  val tripleIntIterable: Iterable[(Int, Int, Int)] = tripleRDD.getTriples.map(f =>
      Uri2IndexT.getUriIndexOrSetT(f.getSubject.toString,f.getPredicate.toString,f.getObject.toString))

  // Apply filtering to remove the (-1,-1,-1) rows
  val tripleIntRDD: RDD[Row] = sparkSession.sparkContext.parallelize(tripleIntIterable.toList).filter(f => f._1 >= 0).map(f => Row(f._1, f._2, f._3))

  val tripleStruct: StructType = new StructType(Array(StructField("s", IntegerType), StructField("p", IntegerType), StructField("o", IntegerType)))
  val tripleDF: DataFrame = sparkSession.createDataFrame(tripleIntRDD, tripleStruct);


  val instanceDF: DataFrame = tripleDF.select("s").distinct().toDF("instance")

  def showDataSets(): Unit = {
    println("")
    println("tripleRDD")
    tripleRDD.getTriples.take(20).foreach(println(_))

    println("")
    println("tripleIntIterable")
    tripleIntIterable.take(20).foreach(println(_))

    println("")
    println("tripleIntRDD")
    tripleIntRDD.take(20).foreach(println(_))

    println("")
    println("tripleDF")
    tripleDF.show()
    println("")
    println("instanceDF")
    instanceDF.show()
  }

  def compute(): DataFrame = {
    val sqlContext = sparkSession.sqlContext

    instanceDF.createOrReplaceTempView("instances")
    tripleDF.createOrReplaceTempView("triples")

    println("Generating Paths from each instance")
    // Generate Paths from each instance
    var df = sqlContext.sql(
      "SELECT i.instance AS instance, CONCAT(t.p, ',', t.o) AS path, t.o " +
        "FROM instances i LEFT JOIN triples t " +
        "WHERE i.instance = t.s")
    df.createOrReplaceTempView("df")

    // TODO: switch loop to recursion? or at least, break when there's no more subpath
    for (i <- 2 to maxDepth) {
      val intermediateDF = sqlContext.sql(
        "SELECT instance, CONCAT(df.path, ',', t.p, ',', t.o) AS path, t.o " +
          "FROM df LEFT JOIN triples t " +
          "WHERE df.o = t.s")
//      if (intermediateDF.count() == 0)

      df = df.union(intermediateDF)
      intermediateDF.createOrReplaceTempView("df")
    }
//    df.show(100)

    println("Aggregate paths")
    //aggregate paths (Strings to Array[String])
    val aggDF = df.drop("o").orderBy("instance").groupBy("instance").agg(collect_list("path") as "paths")
    aggDF.show(100)

    println("compute cvm")
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("paths").setOutputCol("fv").fit(aggDF)
    val cvTrans = cvModel.transform(aggDF)
    // Output can be large, use false to show the whole columns
    // Note: The paths in the output are not ordered
//    cvTrans.show(false)
//    cvTrans.take(20).foreach(println(_))

    // Convert to the correct SparseVector and drop paths
    println("Conversion to correct SparsVector for SVM")
    val dataML = MLUtils.convertVectorColumnsFromML(cvTrans.drop("paths"), "fv")
    dataML.show(20)

    dataML
  }


  def computeLabeledFeatureVectors(): RDD[LabeledPoint] = {

    val dataML: DataFrame = compute()

//  Map to RDD[LabeledPoint] for SVM-support
    val data = dataML.rdd.map { f =>
          val label = Uri2IndexT.uri2prop.getOrElse(f.getInt(0), -1.toInt).toDouble
          val features = f.getAs[org.apache.spark.mllib.linalg.SparseVector](1)
          LabeledPoint(label, features)
        }.filter(f => f.label >= 0)

    println("Labeled Points")
    data.take(20).foreach(println(_))

    data
  }
}


object RDFFastGraphKernel {

  def apply(sparkSession: SparkSession,
            tripleRDD: TripleRDD,
            maxDepth: Int,
            propertyToPredict: String
           ): RDFFastGraphKernel = new RDFFastGraphKernel(sparkSession, tripleRDD, maxDepth, propertyToPredict)

}