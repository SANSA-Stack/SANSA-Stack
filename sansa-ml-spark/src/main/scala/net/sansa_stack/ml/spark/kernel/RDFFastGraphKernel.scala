package net.sansa_stack.ml.spark.kernel

import org.apache.spark.ml.feature.{ CountVectorizer, CountVectorizerModel, StringIndexer }
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.jena.graph.Triple

class RDFFastGraphKernel(
  @transient val sparkSession: SparkSession,
  val tripleRDD:               RDD[Triple],
  val predicateToPredict:      String) extends Serializable {

  import sparkSession.implicits._

  def computeFeatures(): DataFrame = {
    /*
    * Return dataframe schema
    * root
      |-- instance: integer (nullable = true)
      |-- paths: array (nullable = true)
      |    |-- element: string (containsNull = true)
      |-- label: double (nullable = true)
      |-- features: vector (nullable = true)
    * */

    val pathDF: DataFrame = sparkSession.sparkContext
      .parallelize(tripleRDD
        .map(f => (f.getSubject.toString, f.getPredicate.getURI, f.getObject.toString))
        .toLocalIterator.toIterable.toList)
      .map(f => if (f._2 != predicateToPredict) (f._1, "", f._2 + f._3) else (f._1, f._3, ""))
      .toDF("instance", "class", "path")

    // Aggregate paths (Strings to Array[String])
    // Aggregate class labels and filter unassigned subjects
    val aggDF = pathDF.orderBy("instance")
      .groupBy("instance")
      .agg(max("class") as "class", collect_list("path") as "paths")
      .filter("class <> ''")
      .selectExpr("instance", "class", "paths")

    // Transform class to a numerical label
    val indexer = new StringIndexer()
      .setInputCol("class")
      .setOutputCol("label")
      .fit(aggDF)
    val indexedDF = indexer.transform(aggDF).drop("class")

    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("paths").setOutputCol("features").fit(indexedDF)
    val dataML = cvModel.transform(indexedDF)

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

object RDFFastGraphKernel {

  def apply(
    sparkSession:       SparkSession,
    tripleRDD:          RDD[Triple],
    predicateToPredict: String): RDFFastGraphKernel = {

    new RDFFastGraphKernel(sparkSession, tripleRDD, predicateToPredict)
  }

}
