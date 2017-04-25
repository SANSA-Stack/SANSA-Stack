package net.sansa_stack.inference.spark.data

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.utils.{RDFTripleOrdering, RDFTripleToNTripleString}

/**
  * A class that provides methods to write an RDF graph to disk.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphWriter {

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  /**
    * Write the graph to disk in N-Triples format.
    *
    * @param graph the RDF graph
    * @param path the output  directory
    * @param singleFile whether to put all data into a single file
    * @param sorted whether to sort the triples by subject, predicate, object
    */
  def writeToDisk(graph: RDFGraph, path: String, singleFile: Boolean = false, sorted: Boolean = false): Unit = {
    writeTriplesToDisk(graph.triples, path, singleFile, sorted)
  }

  /**
    * Write the triples to disk in N-Triples format.
    *
    * @param triples the triples
    * @param path the output  directory
    * @param singleFile whether to put all data into a single file
    * @param sorted whether to sort the triples by subject, predicate, object
    */
  def writeTriplesToDisk(triples: RDD[RDFTriple],
                         path: String,
                         singleFile: Boolean = false,
                         sorted: Boolean = false): Unit = {
    logger.info("writing triples to disk...")
    val startTime = System.currentTimeMillis()

    implicit val ordering = RDFTripleOrdering

    // sort triples if enabled
    val tmp = if (sorted) {
      triples.map(t => (t, t)).sortByKey().map(_._1)
    } else {
      triples
    }

    // convert to N-Triple format
    var triplesNTFormat = tmp.map(new RDFTripleToNTripleString())

    // convert to single file, i.e. move al lto one partition
    // (might be very expensive and contradicts the Big Data paradigm on Hadoop in general)
    if (singleFile) {
      triplesNTFormat = triplesNTFormat.coalesce(1, shuffle = true)
    }

    // finally, write to disk
    triplesNTFormat.saveAsTextFile(path)

    logger.info("finished writing triples to disk in " + (System.currentTimeMillis() - startTime) + "ms.")
  }

  /**
    * Write the triples represented by the DataFrame to disk in N-Triples format.
    *
    * @param triples the DataFrame containing the triples
    * @param path the output  directory
    * @param singleFile whether to put all data into a single file
    * @param sorted whether to sort the triples by subject, predicate, object
    */
  def writeDataframeToDisk(triples: DataFrame,
                           path: String,
                           singleFile: Boolean = false,
                           sorted: Boolean = false): Unit = {
    writeTriplesToDisk(
      triples.rdd.map(row => RDFTriple(row.getString(0), row.getString(1), row.getString(2))),
      path,
      singleFile,
      sorted
    )
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
  def convertToModel(graph: RDFGraph): Model = {
    val modelString = graph.triples
      .map(new RDFTripleToNTripleString())
      .collect()
      .mkString("\n")

    val model = ModelFactory.createDefaultModel()

    if (!modelString.trim.isEmpty) {
      model.read(new ByteArrayInputStream(modelString.getBytes(StandardCharsets.UTF_8)), null, "N-TRIPLES")
    }

    model
  }
}
