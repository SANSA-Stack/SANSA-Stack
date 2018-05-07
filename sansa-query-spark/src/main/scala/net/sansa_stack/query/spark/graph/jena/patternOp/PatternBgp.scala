package net.sansa_stack.query.spark.graph.jena.patternOp

import net.sansa_stack.query.spark.graph.jena.util.{BasicGraphPattern, ResultMapping}
import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

/**
  * Class for bgp match with target rdf graph.
  * @param triples set of basic patterns.
  */
class PatternBgp(private var triples: Iterator[Triple]) extends PatternOp {

  private val tag = "Bgp Match"
  override def execute(input: Array[Map[Node, Node]],
                       graph: Graph[Node, Node],
                       session: SparkSession): Array[Map[Node, Node]] = {
    if(input.isEmpty) {
      val bgp = new BasicGraphPattern(triples)
      ResultMapping.run(graph, bgp, session)
    } else {
      input
    }
  }

  override def getTag: String = { tag }

  def setBgp(triples: Iterator[Triple]): Unit = {
    this.triples = triples
  }
}
