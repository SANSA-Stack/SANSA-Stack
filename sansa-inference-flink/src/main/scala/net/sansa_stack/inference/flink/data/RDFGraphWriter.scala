package net.sansa_stack.inference.flink.data

import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.jena.rdf.model.{Model, ModelFactory}
import net.sansa_stack.inference.utils.{RDFTripleOrdering, RDFTripleToNTripleString}
import org.slf4j.LoggerFactory

/**
  * Writes an RDF graph to disk.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphWriter {

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def writeToFile(graph: RDFGraph, path: String): Unit = {
    logger.info("writing triples to disk...")
    val startTime  = System.currentTimeMillis()

    implicit val ordering = RDFTripleOrdering

    graph.triples.map(t=>(t,t)).sortPartition(1, Order.DESCENDING).map(_._1)
      .map(new RDFTripleToNTripleString()) // to N-TRIPLES string
      .writeAsText(path, writeMode = FileSystem.WriteMode.OVERWRITE)

    logger.info("finished writing triples to disk in " + (System.currentTimeMillis()-startTime) + "ms.")
  }

  /**
    * Write the graph to disk in N-Triple format.
    *
    * @param graph the RDF graph
    * @param path the output  directory
    * @param singleFile whether to put all data into a single file
    * @param sorted whether to sort the triples by subject, predicate, object
    */
  def writeToDisk(graph: RDFGraph, path: File, singleFile: Boolean = false, sorted: Boolean = false): Unit = {
    logger.info("writing triples to disk...")
    val startTime  = System.currentTimeMillis()

    implicit val ordering = RDFTripleOrdering

    graph.triples.map(t=>(t,t)).sortPartition(1, Order.DESCENDING).map(_._1)
      .map(new RDFTripleToNTripleString()) // to N-TRIPLES string
      .writeAsText(path.getAbsolutePath, writeMode = FileSystem.WriteMode.OVERWRITE)

    logger.info("finished writing triples to disk in " + (System.currentTimeMillis()-startTime) + "ms.")
  }

  def convertToModel(graph: RDFGraph) : Model = {
    val modelString = graph.triples.map(new RDFTripleToNTripleString())
      .collect().mkString("\n")

    val model = ModelFactory.createDefaultModel()

    if(!modelString.trim.isEmpty) {
      model.read(new ByteArrayInputStream(modelString.getBytes(StandardCharsets.UTF_8)), null, "N-TRIPLES")
    }

    model
  }
}
