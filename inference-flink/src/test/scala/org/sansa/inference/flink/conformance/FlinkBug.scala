package org.sansa.inference.flink.conformance

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.sansa.inference.data.RDFTriple
import org.sansa.inference.flink.data.RDFGraph
import org.sansa.inference.flink.forwardchaining.{ForwardRuleReasonerOWLHorst, ForwardRuleReasonerRDFS}

/**
  * Test case for
  *
  * Exception in thread "main" org.apache.flink.optimizer.CompilerException:
  * Bug: Plan generation for Unions picked a ship strategy between binary plan operators.
  *
  * @author Lorenz Buehmann
  */
object FlinkBug {

  def main(args: Array[String]): Unit = {
    val triplesStr =
      "<http://www.example.org#c1> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.example.org#c2> .\n" +
        "<http://www.example.org#w> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.example.org#c1> ."

    val env = ExecutionEnvironment.getExecutionEnvironment
    val conf = new Configuration()
    conf.setString("taskmanager.network.numberOfBuffers", "5000")
    env.getConfig.setGlobalJobParameters(conf)
    val reasoner = new ForwardRuleReasonerRDFS(env)

    val triples = triplesStr.split("\n")
      .map(line => line.replace(">", "").replace("<", "").split("\\s+")) // line to tokens
      .map(tokens => RDFTriple(tokens(0), tokens(1), tokens(2))) // tokens to triple

    val g = RDFGraph(env.fromCollection(triples))

    val g_inf = reasoner.apply(g)


    g_inf.triples.print()

    env.execute()
  }

}
