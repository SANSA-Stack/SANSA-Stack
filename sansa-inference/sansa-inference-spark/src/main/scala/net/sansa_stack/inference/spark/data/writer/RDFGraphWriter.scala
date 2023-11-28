package net.sansa_stack.inference.spark.data.writer


import net.sansa_stack.inference.spark.data.model.RDFGraph
import net.sansa_stack.inference.utils.JenaTripleToNTripleString
import org.aksw.jenax.arq.util.triple.TripleUtils
import org.apache.jena.graph.{GraphUtil, NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory

import java.util.Comparator

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
  def writeTriplesToDisk(triples: RDD[Triple],
                         path: String,
                         singleFile: Boolean = false,
                         sorted: Boolean = false): Unit = {
    logger.info("writing triples to disk...")
    val startTime = System.currentTimeMillis()

    implicit val tripleOrdering = new Ordering[Triple] {
      val comparator: Comparator[Triple] = TripleUtils.compareRDFTerms
      override def compare(t1: Triple, t2: Triple): Int = comparator.compare(t1, t2)
    }

    // sort triples if enabled
    val tmp = if (sorted) {
      triples.map(t => (t, t)).sortByKey().map(_._1)
    } else {
      triples
    }

    // convert to N-Triple format
    var triplesNTFormat = tmp.map(new JenaTripleToNTripleString())

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

    val rowToJenaTriple = (row: Row) => {

      Triple.create(
        NodeFactory.createURI(row.getString(0)),
        NodeFactory.createURI(row.getString(1)),
        if (row.getString(2).startsWith("http:")) NodeFactory.createURI(row.getString(2)) else NodeFactory.createLiteral(row.getString(2)))
    }

    writeTriplesToDisk(
      triples.rdd.map(rowToJenaTriple),
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
    val triples = graph.triples.collect()

    val model = ModelFactory.createDefaultModel()
    GraphUtil.add(model.getGraph, triples)

    model
  }
}
