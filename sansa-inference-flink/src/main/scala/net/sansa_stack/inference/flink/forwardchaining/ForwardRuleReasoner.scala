package net.sansa_stack.inference.flink.forwardchaining

import net.sansa_stack.inference.flink.data.RDFGraph
import org.apache.flink.api.scala.DataSet
import scala.collection.mutable

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.shared.PrefixMapping
import org.apache.jena.sparql.util.FmtUtils

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
    triples.filter(triple => triple.predicateMatches(predicate)).name(s"${FmtUtils.stringForNode(predicate)} triples")
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
//    import net.sansa_stack.inference.utils.PredicateUtils._
//    var extractedTriples = triples
//    var filter = (t: Triple) => true
//
//    if(subject.isDefined) {
//      filter = filter || (_.subjectMatches(subject.get))
////      extractedTriples = extractedTriples.filter(triple => triple.subjectMatches(subject.get))
//    }
//
//    if(predicate.isDefined) {
//      filter = filter || (_.predicateMatches(predicate.get))
////      extractedTriples = extractedTriples.filter(triple => triple.predicateMatches(predicate.get))
//    }
//
//    if(obj.isDefined) {
//      filter = filter || (_.objectMatches(obj.get))
////      extractedTriples = extractedTriples.filter(triple => triple.objectMatches(obj.get))
//    }
//
//    extractedTriples.filter(filter)

    val filterFct = (t: Triple) =>
        t.subjectMatches(subject.orNull) ||
        t.predicateMatches(predicate.orNull) ||
        t.objectMatches(obj.orNull)

    triples.filter(filterFct)
  }

}
