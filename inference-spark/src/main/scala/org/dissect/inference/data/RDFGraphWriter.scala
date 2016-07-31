package org.dissect.inference.data

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.dissect.inference.utils.RDFTripleOrdering
import org.slf4j.LoggerFactory
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Writes an RDF graph to disk.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphWriter {

  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def writeToFile(graph: RDFGraph, path: String): Unit = {
    logger.info("writing triples to disk...")
    val startTime  = System.currentTimeMillis()

    implicit val ordering = RDFTripleOrdering

    graph.triples.map(t=>(t,t)).sortByKey().map(_._1)
      .map(t => "<" + t.subject + "> <" + t.predicate + "> <" + t.`object` + "> .") // to N-TRIPLES string
      .coalesce(1)
      .saveAsTextFile(path)

    logger.info("finished writing triples to disk in " + (System.currentTimeMillis()-startTime) + "ms.")
  }

  def writeToFile(triples: RDD[RDFTriple], path: String): Unit = {
    writeToFile(RDFGraph(triples), path)
  }

  def writeToFile(dataFrame: DataFrame, path: String): Unit = {
    writeToFile(dataFrame.rdd.map(row => RDFTriple(row.getString(0), row.getString(1), row.getString(2))), path)
  }

  def convertToModel(graph: RDFGraph) : Model = {
    val modelString = graph.triples.map(t =>
      "<" + t.subject + "> <" + t.predicate + "> <" + t.`object` + "> .") // to N-TRIPLES string
      .collect().mkString("\n")

    val model = ModelFactory.createDefaultModel()

    if(!modelString.trim.isEmpty) {
      model.read(new ByteArrayInputStream(modelString.getBytes(StandardCharsets.UTF_8)), null, "N-TRIPLES")
    }

    model
  }
}
