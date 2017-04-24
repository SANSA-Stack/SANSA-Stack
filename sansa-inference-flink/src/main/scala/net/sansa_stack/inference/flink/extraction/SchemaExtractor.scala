package net.sansa_stack.inference.flink.extraction

import org.apache.flink.api.scala.DataSet
import org.apache.jena.vocabulary.RDFS

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.flink.data.RDFGraph
import net.sansa_stack.inference.utils.Logging

/**
  * @author Lorenz Buehmann
  */
abstract class SchemaExtractor
  (subjects: Set[String] = Set())
  (predicates: Set[String] = Set())
  (objects: Set[String] = Set())
    extends Logging with Serializable{

  val subjectsFilter: ((RDFTriple) => Boolean) = t => subjects.contains(t.s)
  val predicatesFilter: ((RDFTriple) => Boolean) = t => predicates.contains(t.p)
  val objectsFilter: ((RDFTriple) => Boolean) = t => objects.contains(t.o)

  private def or(ps: (RDFTriple => Boolean)*) = (a: RDFTriple) => ps.exists(_(a))

  /**
    * Extract a graph that contains only the schema triples.
    *
    * @param graph the graph
    * @return a graph containing only the schema triples
    */
  def extract(graph: RDFGraph): RDFGraph =
    new RDFGraph(extract(graph.triples))

  /**
    * Extract a DataSet that contains only the schema triples.
    *
    * @param triples the triples
    * @return the schema triples
    */
  def extract(triples: DataSet[RDFTriple]): DataSet[RDFTriple] =
    triples
      .filter(or(subjectsFilter, predicatesFilter, objectsFilter))
      .name("schema-triples")

}
