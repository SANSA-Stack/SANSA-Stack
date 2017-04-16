package net.sansa_stack.inference.flink.data

import net.sansa_stack.inference.flink.utils.DataSetUtils
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.jena.graph.Triple
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.flink.utils.DataSetUtils.DataSetOps

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
case class RDFGraph(triples: DataSet[RDFTriple]) {

  /**
    * Returns a DataSet of triples that match with the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return DataSet of triples
    */
  def find(s: Option[String] = None, p: Option[String] = None, o: Option[String] = None): DataSet[RDFTriple] = {
    triples.filter(t =>
        (s == None || t.s == s.get) &&
        (p == None || t.p == p.get) &&
        (o == None || t.o == o.get)
    )
  }

  /**
    * Returns a DataSet of triples that match with the given input.
    *
    * @return DataSet of triples
    */
  def find(triple: Triple): DataSet[RDFTriple] = {
    find(
      if (triple.getSubject.isVariable) None else Option(triple.getSubject.toString),
      if (triple.getPredicate.isVariable) None else Option(triple.getPredicate.toString),
      if (triple.getObject.isVariable) None else Option(triple.getObject.toString)
    )
  }

  /**
    * Returns the union of the current RDF graph with the given RDF graph
    *
    * @param other the other RDF graph
    * @return the union of both graphs
    */
  def union(other: RDFGraph): RDFGraph = {
    RDFGraph(triples.union(other.triples))
  }

  /**
    * Returns an RDFGraph with the triples from `this` that are not in `other`.
    *
    * @param other the other RDF graph
    * @return the difference of both graphs
    */
  def subtract(other: RDFGraph): RDFGraph = {
    RDFGraph(triples.subtract(other.triples))
  }

  /**
    * Returns the number of triples.
    *
    * @return the number of triples
    */
  def size() = {
    triples.count()
  }
}
