package net.sansa_stack.inference.flink.forwardchaining

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.flink.data.RDFGraph
import org.apache.flink.api.scala.DataSet

import scala.collection.mutable

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
    * Extracts all triples for the given predicate.
    *
    * @param triples the triples
    * @param predicate the predicate
    * @return the set of triples that contain the predicate
    */
  def extractTriples(triples: mutable.Set[RDFTriple], predicate: String): mutable.Set[RDFTriple] = {
    triples.filter(triple => triple.predicate == predicate)
  }

  /**
    * Extracts all triples for the given predicate.
    *
    * @param triples the DataSet of triples
    * @param predicate the predicate
    * @return the DataSet of triples that contain the predicate
    */
  def extractTriples(triples: DataSet[RDFTriple], predicate: String): DataSet[RDFTriple] = {
    triples.filter(triple => triple.predicate == predicate)
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
  def extractTriples(triples: DataSet[RDFTriple],
                     subject: Option[String],
                     predicate: Option[String],
                     obj: Option[String]): DataSet[RDFTriple] = {
    var extractedTriples = triples

    if(subject.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.subject == subject.get)
    }

    if(predicate.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.predicate == predicate.get)
    }

    if(obj.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.`object` == obj.get)
    }

    extractedTriples
  }

}
