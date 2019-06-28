package net.sansa_stack.inference.flink.data

import java.io.ByteArrayInputStream
import java.net.URI
import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.jena.graph.GraphUtil
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.sparql.graph.GraphFactory
import org.apache.jena.sparql.util.TripleComparator
import org.slf4j.LoggerFactory

import net.sansa_stack.inference.utils.{JenaTripleToNTripleString, RDFTripleOrdering}

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
    val startTime = System.currentTimeMillis()

    implicit val ordering = new TripleComparator()

    graph.triples.map(t => (t, t)).sortPartition(1, Order.DESCENDING).map(_._1)
      .map(new JenaTripleToNTripleString()) // to N-Triples string
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
  def writeToDisk(graph: RDFGraph, path: URI, singleFile: Boolean = false, sorted: Boolean = false): Unit = {
    logger.info("writing triples to disk...")
    val startTime = System.currentTimeMillis()

    implicit val ordering = RDFTripleOrdering

    // sort triples if enabled
    val tmp = if (sorted) {
      graph.triples.sortPartition(_.hashCode(), Order.ASCENDING)
    } else {
      graph.triples
    }

    val sink = tmp
      .map(new JenaTripleToNTripleString()) // to N-TRIPLES string
      .writeAsText(path.toString, writeMode = FileSystem.WriteMode.OVERWRITE)

    // write to single file if enabled
    if (singleFile) {
      sink.setParallelism(1)
    }

    logger.info("finished writing triples to disk in " + (System.currentTimeMillis()-startTime) + "ms.")
  }

  /**
    * Converts an RDF graph to an Apache Jena in-memory model.
    *
    * @note For large graphs this can be too expensive
    * and lead to a OOM exception
    *
    * @param graph the RDF graph
    *
    * @return the in-memory Apache Jena model containing the triples
    */
  def convertToModel(graph: RDFGraph) : Model = {
    val model = ModelFactory.createDefaultModel()
    GraphUtil.add(model.getGraph, graph.triples.collect().toArray)
    model
  }
}
