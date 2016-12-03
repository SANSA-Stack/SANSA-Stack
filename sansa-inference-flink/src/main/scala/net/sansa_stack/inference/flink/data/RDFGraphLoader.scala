package net.sansa_stack.inference.flink.data

import java.io.File

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import net.sansa_stack.inference.data.RDFTriple
import org.apache.flink.configuration.Configuration

/**
  * @author Lorenz Buehmann
  */
object RDFGraphLoader {

  def loadFromFile(path: String, env: ExecutionEnvironment): RDFGraph = {
    val triples = env.readTextFile(path)
      .map(line => line.replace(">", "").replace("<", "").split("\\s+")) // line to tokens
      .map(tokens => RDFTriple(tokens(0), tokens(1), tokens(2))) // tokens to triple

    RDFGraph(triples)
  }

  def loadFromDisk(path: File, env: ExecutionEnvironment): RDFGraph = {
    val paths = if(path.isFile) Seq(path.getAbsolutePath) else path.listFiles().map(f => f.getAbsolutePath).toSeq

    // create a configuration object
    val parameters = new Configuration

    // set the recursive enumeration parameter
    parameters.setBoolean("recursive.file.enumeration", true)

    // pass the configuration to the data source
    val triples = env.readTextFile(path.getAbsolutePath).withParameters(parameters)
      .map(line => line.replace(">", "").replace("<", "").split("\\s+")) // line to tokens
      .map(tokens => RDFTriple(tokens(0), tokens(1), tokens(2))) // tokens to triple

    RDFGraph(triples)
  }

  def loadFromDisk(paths: Seq[File], env: ExecutionEnvironment): RDFGraph = {

    val tmp: Seq[String] = paths.map(path => {
      if(path.isFile) Seq(path.getAbsolutePath) else path.listFiles().map(f => f.getAbsolutePath).toSeq
    }).flatMap(identity)

    val triples = env.fromCollection(tmp)
      .map(line => line.replace(">", "").replace("<", "").split("\\s+")) // line to tokens
      .map(tokens => RDFTriple(tokens(0), tokens(1), tokens(2))) // tokens to triple

    RDFGraph(triples)
  }

}
