package net.sansa_stack.inference.flink.data

import org.apache.flink.api.scala.{DataSet, _}
import org.apache.jena.graph.{Node, Triple}

import net.sansa_stack.inference.flink.utils.DataSetUtils.DataSetOps

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
case class RDFGraph(triples: DataSet[Triple]) {

  val tripleKeyFct : Triple => Int = {t => t.hashCode()}

  /**
    * Returns a DataSet of triples that match with the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return DataSet of triples
    */
  def find(s: Option[Node] = None, p: Option[Node] = None, o: Option[Node] = None): DataSet[Triple] = {
    triples.filter(t =>
        (s.isEmpty || t.subjectMatches(s.get)) &&
        (p.isEmpty || t.predicateMatches(p.get)) &&
        (o.isEmpty || t.objectMatches(o.get))
    )
  }

  /**
    * Returns a DataSet of triples that match with the given input.
    *
    * @return DataSet of triples
    */
  def find(triple: Triple): DataSet[Triple] = {
    find(
      if (triple.getSubject.isVariable) None else Option(triple.getSubject),
      if (triple.getPredicate.isVariable) None else Option(triple.getPredicate),
      if (triple.getObject.isVariable) None else Option(triple.getObject)
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
    RDFGraph(triples.subtract(other.triples, tripleKeyFct, tripleKeyFct))
  }

  /**
    * Returns the number of triples.
    *
    * @return the number of triples
    */
  lazy val size: Long = triples.count()
}
