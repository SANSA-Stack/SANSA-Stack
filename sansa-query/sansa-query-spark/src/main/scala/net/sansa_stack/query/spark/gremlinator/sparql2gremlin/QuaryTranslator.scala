package net.sansa_stack.query.spark.gremlinator.sparql2gremlin

import com.datastax.sparql.gremlin.SparqlToGremlinCompiler
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.{Graph, Vertex}

object QuaryTranslator {

  def apply(sparql: String, graph: Graph): GraphTraversal[Vertex, _] = SparqlToGremlinCompiler.convertToGremlinTraversal(graph, sparql)
}
