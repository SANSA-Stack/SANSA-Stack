package net.sansa_stack.ml.spark.kernel

import org.apache.spark.ml.feature.{ CountVectorizer, CountVectorizerModel, StringIndexer }
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._

class RDFFastTreeGraphKernel_v2(
  @transient val sparkSession: SparkSession,
  val tripleDF: DataFrame,
  val instanceDF: DataFrame,
  val maxDepth: Int) extends Serializable {

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

    tripleDF.cache()
    instanceDF.createOrReplaceTempView("instances")
    tripleDF.createOrReplaceTempView("triples")

    // Generate Paths for each instance
    var pathDF = sqlContext.sql("SELECT instance, label, '' as path, instance as object FROM instances")

    pathDF.createOrReplaceTempView("df")

    for (i <- 1 to maxDepth) {
      // TODO: break the loop when there are no further new paths
      val intermediateDF = sqlContext.sql(
        "SELECT instance, label, CONCAT(df.path, ',', t.predicate, ',', t.object) AS path, t.object " +
          "FROM df LEFT JOIN triples t " +
          "WHERE df.object = t.subject")

      pathDF = pathDF.union(intermediateDF)
      intermediateDF.createOrReplaceTempView("df")
    }

    // Indexing on path
    val indexer = new StringIndexer()
      .setInputCol("path")
      .setOutputCol("pathIndex")
      .fit(pathDF)
    val aggDF = indexer.transform(pathDF).drop("path").drop("object")
      .selectExpr("instance", "label", "cast(pathIndex as string) pathIndex")
      .orderBy("instance")
      .groupBy("instance", "label")
      .agg(collect_list("pathIndex") as "paths")
      .toDF("instance", "label", "paths")

    // CountVectorize the aggregated paths
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("paths").setOutputCol("features").fit(aggDF)
    val dataML = cvModel.transform(aggDF)

    dataML

  }

  def getMLFeatureVectors: DataFrame = {
    /*
      root
      |-- label: double (nullable = true)
      |-- features: vector (nullable = true)
    */

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

object RDFFastTreeGraphKernel_v2 {

  def apply(
    sparkSession: SparkSession,
    tripleDF: DataFrame,
    instanceDF: DataFrame,
    maxDepth: Int): RDFFastTreeGraphKernel_v2 = {

    new RDFFastTreeGraphKernel_v2(sparkSession, tripleDF, instanceDF, maxDepth)
  }

}
