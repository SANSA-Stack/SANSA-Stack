package net.sansa_stack.rdf.flink.data

import java.io.{ ByteArrayInputStream, File }
import java.nio.charset.StandardCharsets

import net.sansa_stack.rdf.flink.utils.Logging
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.jena.rdf.model.{ Model, ModelFactory }
import org.slf4j.LoggerFactory

/**
 * Writes an RDF graph to disk.
 *
 * @author Lorenz Buehmann
 *
 */
object RDFGraphWriter extends Logging {

  def writeToFile(graph: RDFGraph, path: String): Unit = {
    logger.info("writing triples to disk...")
    val startTime = System.currentTimeMillis()

    graph.triples.map(t => (t, t)).map(_._1)
      .map(t => "<" + t.subject + "> <" + t.predicate + "> <" + t.`object` + "> .") // to N-TRIPLES string
      .writeAsText(path, writeMode = FileSystem.WriteMode.OVERWRITE)

    logger.info("finished writing triples to disk in " + (System.currentTimeMillis() - startTime) + "ms.")
  }

  def convertToModel(graph: RDFGraph): Model = {
    val modelString = graph.triples.map(t =>
      "<" + t.subject + "> <" + t.predicate + "> <" + t.`object` + "> .") // to N-TRIPLES string
      .collect().mkString("\n")

    val model = ModelFactory.createDefaultModel()

    if (!modelString.trim.isEmpty) {
      model.read(new ByteArrayInputStream(modelString.getBytes(StandardCharsets.UTF_8)), null, "N-TRIPLES")
    }

    model
  }
}
