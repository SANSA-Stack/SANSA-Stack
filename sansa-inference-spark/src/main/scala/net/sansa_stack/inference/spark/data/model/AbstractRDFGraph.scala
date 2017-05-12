package net.sansa_stack.inference.spark.data.model

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import net.sansa_stack.inference.data.{RDFTriple, SQLSchema, SQLSchemaDefault}

/**
  * A data structure that comprises a collection of triples. Note, due to the implementation of the Spark
  * datastructures, this doesn't necessarily mean to be free of duplicates which is why a `distinct` operation
  * is provided.
  *
  * @author Lorenz Buehmann
  *
  */
abstract class AbstractRDFGraph[T, G <: AbstractRDFGraph[T, G]](val triples: T) { self: G =>


  /**
    * Returns a new RDF graph that contains only triples matching the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return a new RDF graph
    */
  def find(s: Option[Node] = None, p: Option[Node] = None, o: Option[Node] = None): G

  /**
    * Returns a new RDF graph that contains only triples matching the given input.
    *
    * @return a new RDF graph
    */
  def find(triple: Triple): G = {
    find(
      if (triple.getSubject.isVariable) None else Option(triple.getSubject),
      if (triple.getPredicate.isVariable) None else Option(triple.getPredicate),
      if (triple.getObject.isVariable) None else Option(triple.getObject)
    )
  }

  /**
    * Returns a new RDF graph that contains the union of the current RDF graph with the given RDF graph.
    *
    * @param graph the other RDF graph
    * @return the union of both RDF graphs
    */
  def union(graph: G): G

  /**
    * Returns a new RDF graph that contains the union of the current RDF graph with the given RDF graphs.
    *
    * @param graphs the other RDF graphs
    * @return the union of all RDF graphs
    */
  def unionAll(graphs: Seq[G]): G

  /**
    * Returns a new RDF graph that does not contain duplicate triples.
    */
  def distinct(): G

  /**
    * Return the number of triples in the RDF graph.
    *
    * @return the number of triples in the RDF graph
    */
  def size(): Long




  def toDataFrame(sparkSession: SparkSession = null, schema: SQLSchema = SQLSchemaDefault): DataFrame

  def toRDD(): RDD[RDFTriple]

  /**
    * Persist the triples RDD with the default storage level (`MEMORY_ONLY`).
    */
  def cache(): G

//  /**
//    * Broadcast the graph
//    */
//  def broadcast(): G

}
