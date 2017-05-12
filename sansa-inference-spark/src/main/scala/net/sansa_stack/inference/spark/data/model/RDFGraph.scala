package net.sansa_stack.inference.spark.data.model

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import net.sansa_stack.inference.spark.data.model.TripleUtils._



/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
case class RDFGraph (triples: RDD[Triple]) {

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return RDD of triples
    */
  def find(s: Option[Node] = None, p: Option[Node] = None, o: Option[Node] = None): RDD[Triple] = {
      triples.filter(t =>
          (s == None || t.getSubject == s.get) &&
          (p == None || t.getPredicate == p.get) &&
          (o == None || t.getObject == o.get)
      )
  }

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @return RDD of triples
    */
  def find(triple: Triple): RDD[Triple] = {
    find(
      if (triple.getSubject.isVariable) None else Option(triple.getSubject),
      if (triple.getPredicate.isVariable) None else Option(triple.getPredicate),
      if (triple.getObject.isVariable) None else Option(triple.getObject)
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
  def cache(): RDFGraph = {
    triples.cache()
    this
  }

  /**
    * Return the number of triples.
    * @return the number of triples
    */
  def size(): Long = {
    triples.count()
  }



  def toDataFrame(sqlContext: SQLContext): DataFrame = {
    // convert RDD to DataFrame
    val schemaString = "subject predicate object"

    // generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // convert triples RDD to rows
    val rowRDD = triples.map(t => Row(t.s, t.p, t.o))

    // apply the schema to the RDD
    val triplesDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // register the DataFrame as a table
    triplesDataFrame.createOrReplaceTempView("TRIPLES")

    triplesDataFrame
  }
}
