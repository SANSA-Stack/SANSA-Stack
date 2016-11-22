package net.sansa_stack.inference.flink.conformance

import net.sansa_stack.inference.flink.forwardchaining.ForwardRuleReasonerRDFS
import org.apache.flink.api.scala.ExecutionEnvironment
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.flink.data.RDFGraph
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.apache.flink.api.scala._

/**
  * Test context to share an RDFS reasoner.
  *
  * @author Lorenz Buehmann
  */
@RunWith(classOf[JUnitRunner])
class FlinkBugTest extends FlatSpec with BeforeAndAfterAll {

  "this" should "produce the same graph" in {

    val triplesStr =
      "<http://www.example.org#c1> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.example.org#c2> .\n" +
        "<http://www.example.org#w> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.example.org#c1> ."

    val env = ExecutionEnvironment.getExecutionEnvironment
    val reasoner = new ForwardRuleReasonerRDFS(env)

    val triples = triplesStr.split("\n")
      .map(line => line.replace(">", "").replace("<", "").split("\\s+")) // line to tokens
      .map(tokens => RDFTriple(tokens(0), tokens(1), tokens(2))) // tokens to triple

    val g = RDFGraph(env.fromCollection(triples))

    val g_inf = reasoner.apply(g)

    g_inf.triples.print()

  }


}
