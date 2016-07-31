package org.sansa.inference.flink.data

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.sansa.inference.data.RDFTriple

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

}
