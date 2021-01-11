package net.sansa_stack.rdf.flink

import net.sansa_stack.rdf.flink.model.graph.GraphOps
import org.apache.flink.api.scala.DataSet
import org.apache.flink.graph.scala._
import org.apache.jena.graph.{Node, Triple}


/**
 * Wrap up implicit classes/methods for RDF data into [[DataSet]] and [[Graph]].
 *
 * @author Gezim Sejdiu
 */
package object model {

  /**
   * Adds all methods to [[DataSet]] that allows to use TripleOps functions.
   */
  implicit class TripleOperations(triples: DataSet[Triple]) {

    import net.sansa_stack.rdf.flink.model.dataset.TripleOps

    /**
     * Get triples.
     *
     * @return [[DataSet[Triple]]] which contains list of the triples.
     */
    def getTriples(): DataSet[Triple] =
      TripleOps.getTriples(triples)

    /**
     * Get subjects.
     *
     * @return [[DataSet[Node]]] which contains list of the subjects.
     */
    def getSubjects(): DataSet[Node] =
      TripleOps.getSubjects(triples)

    /**
     * Get predicates.
     *
     * @return [[DataSet[Node]]] which contains list of the predicates.
     */
    def getPredicates(): DataSet[Node] =
      TripleOps.getPredicates(triples)

    /**
     * Get objects.
     *
     * @return [[DataSet[Node]]] which contains list of the objects.
     */
    def getObjects(): DataSet[Node] =
      TripleOps.getObjects(triples)

    /**
     * Filter out the subject from a given DataSet[Triple],
     * based on a specific function @func .
     *
     * @param func a partial funtion.
     * @return [[DataSet[Triple]]] a subset of the given DataSet.
     */
    def filterSubjects(func: Node => Boolean): DataSet[Triple] =
      TripleOps.filterSubjects(triples, func)

    /**
     * Filter out the predicates from a given DataSet[Triple],
     * based on a specific function @func .
     *
     * @param func a partial funtion.
     * @return [[DataSet[Triple]]] a subset of the given DataSet.
     */
    def filterPredicates(func: Node => Boolean): DataSet[Triple] =
      TripleOps.filterPredicates(triples, func)

    /**
     * Filter out the objects from a given DataSet[Triple],
     * based on a specific function @func .
     *
     * @param func a partial funtion.
     * @return [[DataSet[Triple]]] a subset of the given DataSet.
     */
    def filterObjects(func: Node => Boolean): DataSet[Triple] =
      TripleOps.filterObjects(triples, func)

    /**
     * Returns an DataSet of triples that match with the given input.
     *
     * @param subject the subject
     * @param predicate the predicate
     * @param object the object
     * @return DataSet of triples
     */
    def find(subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): DataSet[Triple] =
      TripleOps.find(triples, subject, predicate, `object`)

    /**
     * Returns an DataSet of triples that match with the given input.
     *
     * @param triple  the triple to be checked
     * @return DataSet of triples that match the given input
     */
    def find(triple: Triple): DataSet[Triple] =
      TripleOps.find(triples, triple)

    /**
     * Return the number of triples.
     *
     * @return the number of triples
     */
    def size(): Long =
      TripleOps.size(triples)

    /**
     * Return the union of this RDF graph and another one.
     *
     * @param other the other RDF graph
     * @return graph (union of both)
     */
    def union(other: DataSet[Triple]): DataSet[Triple] =
      TripleOps.union(triples, other)

    /**
     * Return the union all of RDF graphs.
     *
     * @param others sequence of DataSets of other RDF graph
     * @return graph (union of all)
     */
    def unionAll(others: Seq[DataSet[Triple]]): DataSet[Triple] = {
      TripleOps.unionAll(triples, others)
    }

    /**
     * Returns a new RDF graph that contains the intersection
     * of the current RDF graph with the given RDF graph.
     *
     * @param other the other RDF graph
     * @return the intersection of both RDF graphs
     */
    def intersect(other: DataSet[Triple]): DataSet[Triple] =
      TripleOps.intersect(triples, other)

    /**
     * Returns a new RDF graph that contains the difference
     * between the current RDF graph and the given RDF graph.
     *
     * @param other the other RDF graph
     * @return the difference of both RDF graphs
     */
    def difference(other: DataSet[Triple]): DataSet[Triple] =
      TripleOps.difference(triples, other)

    /**
     * Determine whether this RDF graph contains any triples
     * with a given (subject, predicate, object) pattern.
     *
     * @param subject the subject (None for any)
     * @param predicate the predicate (None for any)
     * @param object the object (None for any)
     * @return true if there exists within this RDF graph
     * a triple with (S, P, O) pattern, false otherwise
     */
    def contains(subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): Boolean =
      TripleOps.contains(triples, subject, predicate, `object`)

    /**
     * Determine if a triple is present in this RDF graph.
     *
     * @param triple the triple to be checked
     * @return true if the statement s is in this RDF graph, false otherwise
     */
    def contains(triple: Triple): Boolean =
      TripleOps.contains(triples, triple)

    /**
     * Determine if any of the triples in an RDF graph are also contained in this RDF graph.
     *
     * @param other the other RDF graph containing the statements to be tested
     * @return true if any of the statements in RDF graph are also contained
     * in this RDF graph and false otherwise.
     */
    def containsAny(other: DataSet[Triple]): Boolean =
      TripleOps.containsAny(triples, other)

    /**
     * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
     *
     * @param other the other RDF graph containing the statements to be tested
     * @return true if all of the statements in RDF graph are also contained
     * in this RDF graph and false otherwise.
     */
    def containsAll(other: DataSet[Triple]): Boolean =
      TripleOps.containsAll(triples, other)

    /**
     * Add a statement to the current RDF graph.
     *
     * @param triple the triple to be added.
     * @return new DataSet of triples containing this statement.
     */
    def add(triple: Triple): DataSet[Triple] =
      TripleOps.add(triples, triple)

    /**
     * Add a list of statements to the current RDF graph.
     *
     * @param triple the list of triples to be added.
     * @return new DataSet of triples containing this list of statements.
     */
    def addAll(triple: Seq[Triple]): DataSet[Triple] =
      TripleOps.addAll(triples, triple)

    /**
     * Removes a statement from the current RDF graph.
     * The statement with the same subject, predicate and
     * object as that supplied will be removed from the model.
     *
     * @param triple the statement to be removed.
     * @return new DataSet of triples without this statement.
     */
    def remove(triple: Triple): DataSet[Triple] =
      TripleOps.remove(triples, triple)

    /**
     * Removes all the statements from the current RDF graph.
     * The statements with the same subject, predicate and
     * object as those supplied will be removed from the model.
     *
     * @param triple the list of statements to be removed.
     * @return new DataSet of triples without these statements.
     */
    def removeAll(triple: Seq[Triple]): DataSet[Triple] =
      TripleOps.removeAll(triples, triple)

    /**
     * Write N-Triples from a given DataSet of triples
     *
     * @param path path to the file containing N-Triples
     */
    def saveAsNTriplesFile(path: String): Unit =
      TripleOps.saveAsNTriplesFile(triples, path)

  }

  /**
   * Adds methods, `asGraph` to [[DataSet]] that allows to transform as a Graph representation.
   */
  implicit class GraphLoader(triples: DataSet[Triple]) {

    /**
     * Constructs the graph from DataSet of triples
     * @return object of the Graph which contains the constructed  ''graph''.
     */
    def asGraph(): Graph[Long, Node, Node] =
      GraphOps.constructGraph(triples)
  }

  /**
   * Adds methods, `astTriple`, `find`, `size` to [[Graph[Long, Node, Node]] that allows to different operations to it.
   */
  implicit class GraphOperations(graph: Graph[Long, Node, Node]) {

    /**
     * Convert a graph into a DataSet of Triple.
     * @return a DataSet of triples.
     */
    def toDataSet(): DataSet[Triple] =
      GraphOps.toDataSet(graph)

    /**
     * Get triples  of a given graph.
     * @return [[DataSet[Triple]]] which contains list of the graph triples.
     */
    def getTriples(): DataSet[Triple] =
      GraphOps.getTriples(graph)

    /**
     * Get subjects from a given graph.
     * @return [[DataSet[Node]]] which contains list of the subjects.
     */
    def getSubjects(): DataSet[Node] =
      GraphOps.getSubjects(graph)

    /**
     * Get predicates from a given graph.
     * @return [[DataSet[Node]]] which contains list of the predicates.
     */
    def getPredicates(): DataSet[Node] =
      GraphOps.getPredicates(graph)

    /**
     * Get objects from a given graph.
     * @return [[DataSet[Node]]] which contains list of the objects.
     */
    def getObjects(): DataSet[Node] =
      GraphOps.getObjects(graph)

    /**
     * Compute the size of the graph
     * @return the number of edges in the graph.
     */
    def size(): Long =
      GraphOps.size(graph)

    /**
     * Return the union of this graph and another one.
     *
     * @param other of the other graph
     * @return graph (union of all)
     */
    def union(other: Graph[Long, Node, Node]): Graph[Long, Node, Node] =
      GraphOps.union(graph, other)
  }

}
