package net.sansa_stack.inference.flink.data

import java.io.File
import java.net.URI

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import net.sansa_stack.inference.data.RDFTriple
import org.apache.flink.configuration.Configuration
import scala.language.implicitConversions

import org.apache.jena.rdf.model.impl.NTripleReader

import net.sansa_stack.inference.utils.NTriplesStringToRDFTriple

/**
  * @author Lorenz Buehmann
  */
object RDFGraphLoader {

  implicit def pathURIsConverter(uris: Seq[URI]): String = uris.map(p => p.toString).mkString(",")

  def loadFromFile(path: String, env: ExecutionEnvironment): RDFGraph = {
    val triples = env.readTextFile(path)
      .map(line => line.replace(">", "").replace("<", "").split("\\s+")) // line to tokens
      .map(tokens => RDFTriple(tokens(0), tokens(1), tokens(2))) // tokens to triple

    RDFGraph(triples)
  }

  def loadFromDisk(path: URI, env: ExecutionEnvironment): RDFGraph = {
    // create a configuration object
    val parameters = new Configuration

    // set the recursive enumeration parameter
    parameters.setBoolean("recursive.file.enumeration", true)


    // pass the configuration to the data source
    val triples = env.readTextFile(path.toString).withParameters(parameters)
      .map(line => line.replace(">", "").replace("<", "").split("\\s+")) // line to tokens
      .map(tokens => RDFTriple(tokens(0), tokens(1), tokens(2)))
      .name("triples")  // tokens to triple

    RDFGraph(triples)
  }

  def loadFromDisk(paths: Seq[URI], env: ExecutionEnvironment): RDFGraph = {

    val tmp: List[String] = paths.map(path => path.toString).toList

    val converter = new NTriplesStringToRDFTriple()

    val triples = tmp
      .map(f => env.readTextFile(f).flatMap(line => converter.apply(line))).reduce(_ union _).name("triples")

    RDFGraph(triples)
  }

}
