package org.dissect.inference.data

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
case class RDFGraph (triples: RDD[RDFTriple]) {

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return RDD of triples
    */
  def find (s: Option[String] = None, p: Option[String] = None, o: Option[String] = None): RDD[RDFTriple]= {
      triples.filter(t =>
          (s == None || t.subject == s.get) &&
          (p == None || t.predicate == p.get) &&
          (o == None || t.`object` == o.get)
      )
  }

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @return RDD of triples
    */
  def find(triple: Triple): RDD[RDFTriple] = {
    find(
      if (triple.getSubject.isVariable) None else Option(triple.getSubject.toString),
      if (triple.getPredicate.isVariable) None else Option(triple.getPredicate.toString),
      if (triple.getObject.isVariable) None else Option(triple.getObject.toString)
    )
  }

  /**
    * Return the union of the current RDF graph with the given RDF graph
    * @param graph the other RDF graph
    * @return the union of both graphs
    */
  def union(graph: RDFGraph): RDFGraph = {
    RDFGraph(triples.union(graph.triples))
  }

  /**
    * Persist the triples RDD with the default storage level (`MEMORY_ONLY`).
    */
  def cache() = {
    triples.cache()
    this
  }

  /**
    * Return the number of triples.
    * @return the number of triples
    */
  def size() = {
    triples.count()
  }

  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    // convert RDD to DataFrame
    val schemaString = "subject predicate object"

    // generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // convert triples RDD to rows
    val rowRDD = triples.map(t => Row(t.subject, t.predicate, t.`object`))

    // apply the schema to the RDD
    val triplesDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // register the DataFrame as a table
    triplesDataFrame.registerTempTable("TRIPLES")

    triplesDataFrame
  }
}
