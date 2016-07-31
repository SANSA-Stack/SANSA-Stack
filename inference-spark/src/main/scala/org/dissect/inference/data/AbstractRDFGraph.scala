package org.dissect.inference.data

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
abstract class AbstractRDFGraph[T, G <: AbstractRDFGraph[T, G]](triples: T) {

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return RDD of triples
    */
  def find (s: Option[String] = None, p: Option[String] = None, o: Option[String] = None): T

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @return RDD of triples
    */
  def find(triple: Triple): T

  /**
    * Return the union of the current RDF graph with the given RDF graph
    *
    * @param graph the other RDF graph
    * @return the union of both graphs
    */
  def union(graph: G): G

  /**
    * Persist the triples RDD with the default storage level (`MEMORY_ONLY`).
    */
  def cache(): G

  /**
    * Returns a new graph that does not contain duplicate triples.
    */
  def distinct(): G

  /**
    * Return the number of triples.
    *
    * @return the number of triples
    */
  def size(): Long

  def toDataFrame(sparkSession: SparkSession = null): DataFrame

  def toRDD(): RDD[RDFTriple]

}
