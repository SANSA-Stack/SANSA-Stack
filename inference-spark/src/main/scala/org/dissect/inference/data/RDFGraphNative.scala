package org.dissect.inference.data

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFGraphNative(val triples: RDD[RDFTriple]) extends AbstractRDFGraph[RDD[RDFTriple], RDFGraphNative](triples){

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

  def union(graph: RDFGraphNative): RDFGraphNative = {
    new RDFGraphNative(triples.union(graph.toRDD()))
  }

  def cache(): this.type = {
    triples.cache()
    this
  }

  def distinct() = {
    new RDFGraphNative(triples.distinct())
  }

  /**
    * Return the number of triples.
 *
    * @return the number of triples
    */
  def size() = {
    triples.count()
  }

  def toRDD() = triples

  def toDataFrame(sparkSession: SparkSession): DataFrame = {
    // convert RDD to DataFrame
    val schemaString = "subject predicate object"

    // generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // convert triples RDD to rows
    val rowRDD = triples.map(t => Row(t.subject, t.predicate, t.`object`))

    // apply the schema to the RDD
    val triplesDataFrame = sparkSession.createDataFrame(rowRDD, schema)

    // register the DataFrame as a table
    triplesDataFrame.createOrReplaceTempView("TRIPLES")

    triplesDataFrame
  }
}
