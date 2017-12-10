package net.sansa_stack.query.spark.gremlinator.sparql2gremlin

import com.datastax.sparql.gremlin.SparqlToGremlinCompiler
import org.apache.tinkerpop.gremlin.structure.Graph

object QuaryTranslator {

  def apply(sparql: String, graph: Graph) = SparqlToGremlinCompiler.convertToGremlinTraversal(graph, sparql)
}