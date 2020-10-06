package net.sansa_stack.rdf.common.partition.model.sparqlify

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.common.partition.model.sparqlify.SparqlifyUtils2._
import org.aksw.obda.jena.domain.impl.ViewDefinition
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.scalatest.FunSuite

/**
 * @author Gezim Sejdiu
 */
class SparqlifyUtils2Tests extends FunSuite {

  val triple = Triple.create(
    NodeFactory.createURI("http://dbpedia.org/resource/Germany"),
    NodeFactory.createURI("http://dbpedia.org/ontology/populationTotal"),
    NodeFactory.createLiteral("82175700"))

  test("creating view definition should pass") {
    val partitioner = RdfPartitionerDefault.fromTriple(triple)
    val viewDefinition = createViewDefinition(partitioner)
    val expectedViewDefinition = new ViewDefinition(viewDefinition.getName, viewDefinition.getConstructTemplate, viewDefinition.getVarDefinition, viewDefinition.getConstraints, viewDefinition.getLogicalTable)
    assert(createViewDefinition(partitioner).equals(expectedViewDefinition))
  }

}
