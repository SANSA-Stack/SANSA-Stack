package net.sansa_stack.ml.spark.kernel

import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, StringIndexer}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object RDFFastTreeGraphKernelUtil {

  def triplesToDF(sparkSession: SparkSession,
                  triples: RDD[graph.Triple],
                  subjectColName:String = "subject",
                  predicateColName:String = "predicate",
                  objectColName:String ="object"
                 ): DataFrame = {
    import sparkSession.implicits._

    triples.map(f => (f.getSubject.toString,f.getPredicate.toString,f.getObject.toString))
      .toDF(subjectColName, predicateColName, objectColName)
  }

  def getInstanceAndLabelDF( filteredTripleDF: DataFrame,
                             subjectColName:String = "subject",
                             objectColName:String ="object" ): DataFrame = {
    /*
      root
      |-- instance: string (nullable = true)
      |-- label: double (nullable = true)
    */

    val df = filteredTripleDF.select(subjectColName, objectColName).distinct()

    val indexer = new StringIndexer()
      .setInputCol(objectColName)
      .setOutputCol("label")
      .fit(df)
    val indexedDF = indexer.transform(df).drop(objectColName)
      .groupBy(subjectColName)
      .agg(max("label") as "label")
      .toDF("instance", "label")

    indexedDF
  }
}
