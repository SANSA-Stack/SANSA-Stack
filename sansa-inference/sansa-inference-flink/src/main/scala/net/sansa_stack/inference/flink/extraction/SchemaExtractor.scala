package net.sansa_stack.inference.flink.extraction

import org.apache.flink.api.scala.DataSet
import org.apache.jena.graph.{Node, Triple}

import net.sansa_stack.inference.flink.data.RDFGraph
import net.sansa_stack.inference.utils.Logging

/**
  * @author Lorenz Buehmann
  */
abstract class SchemaExtractor
  (subjects: Set[Node] = Set())
  (predicates: Set[Node] = Set())
  (objects: Set[Node] = Set())
    extends Logging with Serializable{

  def subjectsFilter: ((Triple) => Boolean) = t => subjects.contains(t.getSubject)
  val predicatesFilter: ((Triple) => Boolean) = t => predicates.contains(t.getPredicate)
  val objectsFilter: ((Triple) => Boolean) = t => objects.contains(t.getObject)

  private def or(ps: (Triple => Boolean)*) = (a: Triple) => ps.exists(_(a))

  /**
    * Extract a graph that contains only the schema triples.
    *
    * @param graph the graph
    * @return a graph containing only the schema triples
    */
  def extract(graph: RDFGraph): RDFGraph = RDFGraph(extract(graph.triples))

  /**
    * Extract a DataSet that contains only the schema triples.
    *
    * @param triples the triples
    * @return the schema triples
    */
  def extract(triples: DataSet[Triple]): DataSet[Triple] =
    triples
      .filter(or(subjectsFilter, predicatesFilter, objectsFilter))
      .name("schema-triples")

}
