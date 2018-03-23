package net.sansa_stack.rdf.spark.io

import java.io.ByteArrayInputStream
import java.net.URI

import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession




/**
  * @author Lorenz Buehmann
  */
class RDFReader(implicit val session: SparkSession) {

  import net.sansa_stack.rdf.spark.io._

  /**
    * Load RDF data into an [[RDD]][Triple].
    * Currently, only N-Triples and Turtle syntax are supported.
    *
    * @param path the path to the RDF data file(s)
    * @param lang the RDF language (Turtle or N-Triples)
    * @return the [[RDD]] of triples
    */
  def load(path: URI, lang: RDFLang.Value): RDD[Triple] = {
//    session.sparkContext.rdf()
    load(session, path.toString)
  }

  /**
    * Loads an N-Triples file into an RDD.
    *
    * @param session the Spark session
    * @param path    the path to the N-Triples file(s)
    * @return the RDD of triples
    */
  def load(session: SparkSession, path: String): RDD[Triple] = {
    session.sparkContext.textFile(path)
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
      .map(line =>
        RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
  }

}


