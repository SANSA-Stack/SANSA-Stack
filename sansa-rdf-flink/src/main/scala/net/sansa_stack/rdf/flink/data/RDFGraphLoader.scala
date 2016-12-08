package net.sansa_stack.rdf.flink.data

import java.io.File

import org.apache.flink.api.scala.{ ExecutionEnvironment, _ }
import org.apache.flink.configuration.Configuration
import org.apache.jena.riot.RDFDataMgr
import java.io.ByteArrayInputStream
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.flink.model.RDFTriple

/**
 * @author Gezim Sejdiu
 */
object RDFGraphLoader {

  def loadFromFile(path: String, env: ExecutionEnvironment): RDFGraph = {
    val triples = env.readTextFile(path)
      .map { line =>
        val it = RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next()
        RDFTriple(it.getSubject, it.getPredicate, it.getObject)
      }

    RDFGraph(triples)
  }
}
