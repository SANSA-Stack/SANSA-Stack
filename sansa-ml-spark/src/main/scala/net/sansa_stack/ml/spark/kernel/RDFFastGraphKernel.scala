package net.sansa_stack.ml.spark.kernel

import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, StringIndexer}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


class RDFFastGraphKernel(@transient val sparkSession: SparkSession,
                         val tripleRDD: TripleRDD,
                         val predicateToPredict: String
                        ) extends Serializable {

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
      .parallelize(tripleRDD.getTriples.map(f => (f.getSubject.toString,f.getPredicate.toString,f.getObject.toString)).toList)
      .map(f => if(f._2 != predicateToPredict) (f._1,-1,f._2+f._3) else (f._1,f._3.hashCode(),""))
      .toDF("instance", "hash_label", "path")

    //aggregate paths (Strings to Array[String])
    //aggregate hash_labels to maximum per subject and filter unassigned subjects
    val aggDF = pathDF.orderBy("instance")
      .groupBy("instance")
      .agg(max("hash_label") as "hash_label",collect_list("path") as "paths")
      .filter("hash_label>=0")
      //cast hash_label from int to string to use StringIndexer later on
      .selectExpr("instance","cast(hash_label as string) hash_label","paths")


    val indexer = new StringIndexer()
    .setInputCol("hash_label")
    .setOutputCol("label")
    .fit(aggDF)
    val indexedDF = indexer.transform(aggDF).drop("hash_label")


    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("paths").setOutputCol("features").fit(indexedDF)
    val dataML = cvModel.transform(indexedDF)

    dataML.printSchema()
    dataML.show(20)

    dataML.select("instance", "label").groupBy("label").count().show()

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
            predicateToPredict: String
           ): RDFFastGraphKernel = {

    new RDFFastGraphKernel(sparkSession, tripleRDD, predicateToPredict)
  }

}