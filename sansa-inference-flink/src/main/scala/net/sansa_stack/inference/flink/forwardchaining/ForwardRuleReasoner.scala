package net.sansa_stack.inference.flink.forwardchaining

import net.sansa_stack.inference.flink.data.RDFGraph
import org.apache.flink.api.scala.DataSet
import scala.collection.mutable

import org.apache.jena.graph.{Node, Triple}

/**
  * A forward chaining based reasoner.
  *
  * @author Lorenz Buehmann
  */
trait ForwardRuleReasoner extends TransitiveReasoner{

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  def apply(graph: RDFGraph) : RDFGraph

  /**
    * Applies forward chaining to the given set of RDF triples and returns a new set of RDF triples that
    * contains all additional triples based on the underlying set of rules.
    *
    * @param triples the RDF triples
    * @return the materialized RDF triples
    */
  def apply(triples: DataSet[Triple]) : DataSet[Triple] = apply(RDFGraph(triples)).triples

  /**
    * Extracts all triples for the given predicate.
    *
    * @param triples the triples
    * @param predicate the predicate
    * @return the set of triples that contain the predicate
    */
  def extractTriples(triples: mutable.Set[Triple], predicate: Node): mutable.Set[Triple] = {
    triples.filter(triple => triple.predicateMatches(predicate))
  }

  /**
    * Extracts all triples for the given predicate.
    *
    * @param triples the DataSet of triples
    * @param predicate the predicate
    * @return the DataSet of triples that contain the predicate
    */
  def extractTriples(triples: DataSet[Triple], predicate: Node): DataSet[Triple] = {
    triples.filter(triple => triple.predicateMatches(predicate))
  }

  /**
    * Extracts all triples that match the given subject, predicate and object if defined.
    *
    * @param triples the DataSet of triples
    * @param subject the subject
    * @param predicate the predicate
    * @param obj the object
    * @return the DataSet of triples that match
    */
  def extractTriples(triples: DataSet[Triple],
                     subject: Option[Node],
                     predicate: Option[Node],
                     obj: Option[Node]): DataSet[Triple] = {
    var extractedTriples = triples

    if(subject.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.subjectMatches(subject.get))
    }

    if(predicate.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.predicateMatches(predicate.get))
    }

    if(obj.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.objectMatches(obj.get))
    }

    extractedTriples
  }

}
