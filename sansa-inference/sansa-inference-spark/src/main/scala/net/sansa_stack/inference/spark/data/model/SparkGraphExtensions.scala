package net.sansa_stack.inference.spark.data.model

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import net.sansa_stack.inference.data._

/**
  * Some Spark based extension for an RDF graph.
  *
  * @author Lorenz Buehmann
  */
trait SparkGraphExtensions[Rdf <: RDF, D, G <: AbstractRDFGraph[Rdf, D, G]] {

  /**
    * Convert the current graph to a Dataframe of RDF triples.
    *
    * @param sparkSession the Spark session
    * @param schema       the SQL schema
    * @return a Dataframe of RDF triples
    */
  def toDataFrame(sparkSession: SparkSession = null, schema: SQLSchema = SQLSchemaDefault): DataFrame

  /**
    * Convert the current graph to an RDD of RDF triples.
    *
    * @return RDD of RDF triples
    */
  def toRDD(): RDD[Rdf#Triple]

  /**
    * Persist the triples RDD with the default storage level (`MEMORY_ONLY`).
    */
  def cache(): G

}
