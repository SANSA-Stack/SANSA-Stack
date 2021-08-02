package net.sansa_stack.ml.spark.similarity.similarityEstimationModels

import net.sansa_stack.ml.spark.featureExtraction.{SmartFeatureExtractor, SparqlFrame}
import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class DaSimEstimator {

  // parameter

  // intial filter
  var _pInitialFilterBySPARQL: String = null
  var _pInitialFilterByObject: String = null

  val _parameter_distSimFeatureExtractionMethod = "os"
  val _parameterVerboseProcess = false


  def setSparqlFilter(sparqlFilter: String): this.type = {
    _pInitialFilterBySPARQL = sparqlFilter
    this
  }

  def setObjectFilter(objectFilter: String): this.type = {
    _pInitialFilterByObject = objectFilter
    this
  }

  def gatherSeeds(ds: Dataset[Triple], sparqlFilter: String = null, objectFilter: String = null): DataFrame = {

    val spark = SparkSession.builder.getOrCreate()

    val seeds: DataFrame = {
    if (objectFilter!= null) {
      ds
        .filter (t => ((t.getObject.toString ().equals (objectFilter) ) ) )
        .rdd
        .toDF()
        .select("s")
        .withColumnRenamed("s", "seed")
    }
    else if (sparqlFilter != null) {
      val sf = new SparqlFrame()
        .setSparqlQuery(sparqlFilter)
      val tmpDf = sf
        .transform(ds)
      val cn: Array[String] = tmpDf.columns
      tmpDf
        .withColumnRenamed(cn(0), "seed")
    }
    else {
      val tmpSchema = new StructType()
        .add(StructField("seed", StringType, true))

      spark.createDataFrame(
        ds
          .rdd
          .flatMap(t => Seq(t.getSubject, t.getObject))
          .filter(_.isURI)
          .map(_.toString())
          .distinct
          .map(Row(_)),
        tmpSchema
      )
    }
  }
    // assert(seeds.columns == Array("seed"))
  seeds
  }

  def gatherFeatures(ds: Dataset[Triple], seeds: DataFrame, sparqlFeatureExtractionQuery: String = null): Unit = {
    val featureDf = {
      if (sparqlFeatureExtractionQuery != null) {
        println("DaSimEstimator: Feature Extraction by SparqlFrame")
        val sf = new SparqlFrame()
          .setSparqlQuery(sparqlFeatureExtractionQuery)
        val tmpDf = sf
          .transform(ds)
        val cn: Array[String] = tmpDf.columns
        tmpDf
      }
      else {
        println("DaSimEstimator: Feature Extraction by SmartFeatureExtractor")

        implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]

        val filtered: Dataset[Triple] = seeds
          .rdd
          .map(r => Tuple2(r(0).toString, r(0)))
          .join(ds.rdd.map(t => Tuple2(t.getSubject.toString(), t)))
          .map(_._2._2)
          .toDS()
          .as[Triple]

        val sfe = new SmartFeatureExtractor()
          .setEntityColumnName("s")
        val feDf = sfe
          .transform(filtered)
        feDf
      }
    }
  }

  def transform(dataset: Dataset[Triple]): DataFrame = {
    // gather seeds
    val seeds: DataFrame = gatherSeeds(dataset, _pInitialFilterBySPARQL, _pInitialFilterByObject)
    seeds
    // filter in
  }
}
