package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Node, Triple }
import net.sansa_stack.rdf.spark.model.rdd.TripleOps

/**
 * Wrap up implicit classes/methods for RDF data into [[RDD]].
 *
 * @author Gezim Sejdiu
 */

package object model {

  /**
   * Adds all methods to [[RDD]] that allows to use TripleOps functions.
   */
  implicit class TripleOperations(triples: RDD[Triple]) extends Logging {

    /**
     * Convert a [[RDD[Triple]]] into a DataFrame.
     * @return a DataFrame of triples.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.toDF]]
     */
    def toDF() =
      TripleOps.toDF(triples)

    /**
     * Convert an RDD of Triple into a Dataset of Triple.
     * @return a Dataset of triples.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.toDS]]
     */
    def toDS() = TripleOps.toDS(triples)

    /**
     * Get subjects.
     * @return [[RDD[Node]]] which contains list of the subjects.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.getSubjects]]
     */
    def getSubjects() = TripleOps.getSubjects(triples)

    /**
     * Get predicates.
     * @return [[RDD[Node]]] which contains list of the predicates.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.getPredicates]]
     */
    def getPredicates() = TripleOps.getPredicates(triples)

    /**
     * Get objects.
     * @return [[RDD[Node]]] which contains list of the objects.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.getObjects]]
     */
    def getObjects() = TripleOps.getObjects(triples)

    /**
     * Filter out the subject from a given RDD[Triple],
     * based on a specific function @func .
     * @param func a partial funtion.
     * @return [[RDD[Triple]]] a subset of the given RDD.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.filterSubjects]]
     */
    def filterSubjects(func: Node => Boolean) =
      TripleOps.filterSubjects(triples, func)

    /**
     * Filter out the predicates from a given RDD[Triple],
     * based on a specific function @func .
     * @param func a partial funtion.
     * @return [[RDD[Triple]]] a subset of the given RDD.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.filterPredicates]]
     */
    def filterPredicates(func: Node => Boolean) =
      TripleOps.filterPredicates(triples, func)

    /**
     * Filter out the objects from a given RDD[Triple],
     * based on a specific function @func .
     * @param func a partial funtion.
     * @return [[RDD[Triple]]] a subset of the given RDD.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.filterObjects]]
     */
    def filterObjects(func: Node => Boolean) =
      TripleOps.filterObjects(triples, func)

    /**
     * Returns an RDD of triples that match with the given input.
     *
     * @param subject the subject
     * @param predicate the predicate
     * @param object the object
     * @return RDD of triples
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.find]]
     */
    def find(subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None) =
      TripleOps.find(triples, subject, predicate, `object`)

    /**
     * Returns an RDD of triples that match with the given input.
     *
     * @param triple the triple to be checked
     * @return RDD of triples that match the given input
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.find]]
     */
    def find(triple: Triple): RDD[Triple] = TripleOps.find(triples, triple)

    /**
     * Determine whether this RDF graph contains any triples
     * with a given subject and predicate.
     *
     * @param subject the subject (None for any)
     * @param predicate the predicate (Node for any)
     * @return true if there exists within this RDF graph
     * a triple with subject and predicate, false otherwise
     */
    def contains(subject: Some[Node], predicate: Some[Node]) =
      TripleOps.contains(triples, subject, predicate, None)

    /**
     * Determine whether this RDF graph contains any triples
     * with a given (subject, predicate, object) pattern.
     *
     * @param subject the subject (None for any)
     * @param predicate the predicate (Node for any)
     * @param object the object (Node for any)
     * @return true if there exists within this RDF graph
     * a triple with (S, P, O) pattern, false otherwise
     */
    def contains(subject: Some[Node], predicate: Some[Node], `object`: Some[Node]) =
      TripleOps.contains(triples, subject, predicate, `object`)

    /**
     * Determine if a triple is present in this RDF graph.
     *
     * @param triple the triple to be checked
     * @return true if the statement s is in this RDF graph, false otherwise
     */
    def contains(triple: Triple) = TripleOps.contains(triples, triple)

    /**
     * Determine if any of the triples in an RDF graph are also contained in this RDF graph.
     *
     * @param other the other RDF graph containing the statements to be tested
     * @return true if any of the statements in RDF graph are also contained
     * in this RDF graph and false otherwise.
     */
    def containsAny(other: RDD[Triple]) = TripleOps.containsAny(triples, other)

    /**
     * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
     *
     * @param other the other RDF graph containing the statements to be tested
     * @return true if all of the statements in RDF graph are also contained
     * in this RDF graph and false otherwise.
     */
    def containsAll(other: RDD[Triple]) = TripleOps.containsAll(triples, other)

    /**
     * Return the number of triples.
     * @return the number of triples
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.size]]
     */
    def size() = TripleOps.size(triples)

    /**
     * Return the union of this RDF graph and another one.
     *
     * @param other of the other RDF graph
     * @return graph (union of both)
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.union]]
     */
    def union(other: RDD[Triple]) = TripleOps.union(triples, other)

    /**
     * Return the union all of RDF graphs.
     *
     * @return graph (union of all)
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.unionAll]]
     */
    def unionAll(others: Seq[RDD[Triple]]) =
      TripleOps.unionAll(triples, others)

    /**
     * Returns a new RDF graph that contains the intersection of the current RDF graph with the given RDF graph.
     *
     * @param other of the other RDF graph
     * @return the intersection of both RDF graphs
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.intersection]]
     */
    def intersection(other: RDD[Triple]) =
      TripleOps.intersection(triples, other)
    /**
     * Returns a new RDF graph that contains the difference between the current RDF graph and the given RDF graph.
     *
     * @param other of the other RDF graph
     * @return the difference of both RDF graphs
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.difference]]
     */
    def difference(other: RDD[Triple]) = TripleOps.difference(triples, other)

    /**
     * Add a statement to the current RDF graph.
     *
     * @param triple the triple to be added.
     * @return new RDD of triples containing this statement.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.add]]
     */
    def add(triple: Triple) = TripleOps.add(triples, triple)

    /**
     * Add a list of statements to the current RDF graph.
     *
     * @param triple the list of triples to be added.
     * @return new RDD of triples containing this list of statements.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.addAll]]
     */
    def addAll(triple: Seq[Triple]) = TripleOps.addAll(triples, triple)

    /**
     * Removes a statement from the current RDF graph.
     * The statement with the same subject, predicate and object as that supplied will be removed from the model.
     * @param triple the statement to be removed.
     * @return new RDD of triples without this statement.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.remove]]
     */
    def remove(triple: Triple) = TripleOps.remove(triples, triple)

    /**
     * Removes all the statements from the current RDF graph.
     * The statements with the same subject, predicate and object as those supplied will be removed from the model.
     * @param triple the list of statements to be removed.
     * @return new RDD of triples without these statements.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.removeAll]]
     */
    def removeAll(triple: Seq[Triple]) = TripleOps.removeAll(triples, triple)

    /**
     * Write N-Triples from a given RDD of triples
     *
     * @param path path to the file containing N-Triples
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.saveAsNTriplesFile]]
     */
    def saveAsNTriplesFile(path: String) =
      TripleOps.saveAsNTriplesFile(triples, path)

  }

}