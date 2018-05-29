package net.sansa_stack.query.spark.graph.jena.util

import org.apache.jena.graph.Triple
import org.apache.spark.SparkContext

/**
  * Class that generate GraphX graph for input triples
  *
  * @param triples input triples to generate graph patterns
  *
  * @author Zhe Wang
  */
class BasicGraphPattern(triples: Iterator[Triple]) extends Serializable {

  val triplePatterns: List[TriplePatternNode] = {
    triples.toList.map( t => new TriplePatternNode(t.getSubject, t.getPredicate, t.getObject))
  }

  lazy val numTriples: Long = triplePatterns.length
}

object BasicGraphPattern{
  def apply(triples: Iterator[Triple], sc: SparkContext): BasicGraphPattern = new BasicGraphPattern(triples)
}
