package net.sansa_stack.rdf.flink.model.dataset

import net.sansa_stack.rdf.flink.utils.DataSetUtils.DataSetOps
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.jena.graph.{Node, Triple}

/**
 * Flink based implementation of DataSet[Triple].
 *
 * @author Gezim Sejdiu
 */
object TripleOps {

  /**
   * Get triples.
   *
   * @param triples DataSet of triples.
   * @return [[DataSet[Triple]]] which contains list of the triples.
   */
  def getTriples(triples: DataSet[Triple]): DataSet[Triple] =
    triples

  /**
   * Get subjects.
   *
   * @param triples DataSet of triples.
   * @return [[DataSet[Node]]] which contains list of the subjects.
   */
  def getSubjects(triples: DataSet[Triple]): DataSet[Node] =
    triples.map(_.getSubject)

  /**
   * Get predicates.
   *
   * @param triples DataSet of triples.
   * @return [[DataSet[Node]]] which contains list of the predicates.
   */
  def getPredicates(triples: DataSet[Triple]): DataSet[Node] =
    triples.map(_.getPredicate)

  /**
   * Get objects.
   *
   * @param triples DataSet of triples.
   * @return [[DataSet[Node]]] which contains list of the objects.
   */
  def getObjects(triples: DataSet[Triple]): DataSet[Node] =
    triples.map(_.getObject)

  /**
   * Filter out the subject from a given DataSet[Triple],
   * based on a specific function @func .
   *
   * @param triples DataSet of triples.
   * @param func a partial funtion.
   * @return [[DataSet[Triple]]] a subset of the given DataSet.
   */
  def filterSubjects(triples: DataSet[Triple], func: Node => Boolean): DataSet[Triple] =
    triples.filter(f => func(f.getSubject))

  /**
   * Filter out the predicates from a given DataSet[Triple],
   * based on a specific function @func .
   *
   * @param triples DataSet of triples.
   * @param func a partial funtion.
   * @return [[DataSet[Triple]]] a subset of the given DataSet.
   */
  def filterPredicates(triples: DataSet[Triple], func: Node => Boolean): DataSet[Triple] =
    triples.filter(f => func(f.getPredicate))

  /**
   * Filter out the objects from a given DataSet[Triple],
   * based on a specific function @func .
   *
   * @param triples DataSet of triples.
   * @param func a partial funtion.
   * @return [[DataSet[Triple]]] a subset of the given DataSet.
   */
  def filterObjects(triples: DataSet[Triple], func: Node => Boolean): DataSet[Triple] =
    triples.filter(f => func(f.getSubject))

  /**
   * Returns an DataSet of triples that match with the given input.
   *
   * @param triples DataSet of triples
   * @param subject the subject
   * @param predicate the predicate
   * @param object the object
   * @return DataSet of triples
   */
  def find(triples: DataSet[Triple], subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): DataSet[Triple] = {
    triples.filter(t =>
      (subject == None || t.getSubject.matches(subject.get)) &&
        (predicate == None || t.getPredicate.matches(predicate.get)) &&
        (`object` == None || t.getObject.matches(`object`.get)))
  }

  /**
   * Returns an DataSet of triples that match with the given input.
   *
   * @param triples DataSet of triples
   * @param triple  the triple to be checked
   * @return DataSet of triples that match the given input
   */
  def find(triples: DataSet[Triple], triple: Triple): DataSet[Triple] = {
    find(
      triples,
      if (triple.getSubject.isVariable) None else Option(triple.getSubject),
      if (triple.getPredicate.isVariable) None else Option(triple.getPredicate),
      if (triple.getObject.isVariable) None else Option(triple.getObject))
  }

  /**
   * Return the number of triples.
   *
   * @param triples DataSet of triples
   * @return the number of triples
   */
  def size(triples: DataSet[Triple]): Long =
    triples.count()

  /**
   * Return the union of this RDF graph and another one.
   *
   * @param triples DataSet of RDF graph
   * @param other the other RDF graph
   * @return graph (union of both)
   */
  def union(triples: DataSet[Triple], other: DataSet[Triple]): DataSet[Triple] =
    triples.union(other)

  /**
   * Return the union all of RDF graphs.
   *
   * @param triples DataSet of RDF graph
   * @param others sequence of DataSets of other RDF graph
   * @return graph (union of all)
   */
  def unionAll(triples: DataSet[Triple], others: Seq[DataSet[Triple]]): DataSet[Triple] = {
    val first = others.head
    first.union(triples)
  }

  /**
   * Returns a new RDF graph that contains the intersection
   * of the current RDF graph with the given RDF graph.
   *
   * @param triples DataSet of RDF graph
   * @param other the other RDF graph
   * @return the intersection of both RDF graphs
   */
  def intersect(triples: DataSet[Triple], other: DataSet[Triple]): DataSet[Triple] =
    triples.intersect(other)

  /**
   * Returns a new RDF graph that contains the difference
   * between the current RDF graph and the given RDF graph.
   *
   * @param triples DataSet of RDF graph
   * @param other the other RDF graph
   * @return the difference of both RDF graphs
   */
  def difference(triples: DataSet[Triple], other: DataSet[Triple]): DataSet[Triple] =
    triples.subtract(other)

  /**
   * Determine whether this RDF graph contains any triples
   * with a given (subject, predicate, object) pattern.
   *
   * @param triples DataSet of triples
   * @param subject the subject (None for any)
   * @param predicate the predicate (None for any)
   * @param object the object (None for any)
   * @return true if there exists within this RDF graph
   * a triple with (S, P, O) pattern, false otherwise
   */
  def contains(triples: DataSet[Triple], subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): Boolean = {
    find(triples, subject, predicate, `object`).count() > 0
  }

  /**
   * Determine if a triple is present in this RDF graph.
   *
   * @param triples DataSet of triples
   * @param triple the triple to be checked
   * @return true if the statement s is in this RDF graph, false otherwise
   */
  def contains(triples: DataSet[Triple], triple: Triple): Boolean = {
    find(triples, triple).count() > 0
  }

  /**
   * Determine if any of the triples in an RDF graph are also contained in this RDF graph.
   *
   * @param triples DataSet of triples
   * @param other the other RDF graph containing the statements to be tested
   * @return true if any of the statements in RDF graph are also contained
   * in this RDF graph and false otherwise.
   */
  def containsAny(triples: DataSet[Triple], other: DataSet[Triple]): Boolean = {
    difference(triples, other).count() > 0
  }

  /**
   * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
   *
   * @param triples DataSet of triples
   * @param other the other RDF graph containing the statements to be tested
   * @return true if all of the statements in RDF graph are also contained
   * in this RDF graph and false otherwise.
   */
  def containsAll(triples: DataSet[Triple], other: DataSet[Triple]): Boolean = {
    difference(triples, other).count() == 0
  }

  @transient var env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  import scala.jdk.CollectionConverters._

  /**
   * Add a statement to the current RDF graph.
   *
   * @param triples DataSet of RDF graph
   * @param triple the triple to be added.
   * @return new DataSet of triples containing this statement.
   */
  def add(triples: DataSet[Triple], triple: Triple): DataSet[Triple] = {
    val statement = env.fromCollection(Seq(triple).asJava).asInstanceOf[DataSet[Triple]]
    union(triples, statement)
  }

  /**
   * Add a list of statements to the current RDF graph.
   *
   * @param triples DataSet of RDF graph
   * @param triple the list of triples to be added.
   * @return new DataSet of triples containing this list of statements.
   */
  def addAll(triples: DataSet[Triple], triple: Seq[Triple]): DataSet[Triple] = {
    val statements = env.fromCollection(triple.asJava).asInstanceOf[DataSet[Triple]]
    union(triples, statements)
  }

  /**
   * Removes a statement from the current RDF graph.
   * The statement with the same subject, predicate and
   * object as that supplied will be removed from the model.
   *
   * @param triples DataSet of RDF graph
   * @param triple the statement to be removed.
   * @return new DataSet of triples without this statement.
   */
  def remove(triples: DataSet[Triple], triple: Triple): DataSet[Triple] = {
    val statement = env.fromCollection(Seq(triple).asJava).asInstanceOf[DataSet[Triple]]
    difference(triples, statement)
  }

  /**
   * Removes all the statements from the current RDF graph.
   * The statements with the same subject, predicate and
   * object as those supplied will be removed from the model.
   *
   * @param triples DataSet of RDF graph
   * @param triple the list of statements to be removed.
   * @return new DataSet of triples without these statements.
   */
  def removeAll(triples: DataSet[Triple], triple: Seq[Triple]): DataSet[Triple] = {
    val statements = env.fromCollection(triple.asJava).asInstanceOf[DataSet[Triple]]
    difference(triples, statements)
  }

  /**
   * Write N-Triples from a given DataSet of triples
   *
   * @param triples DataSet of RDF graph
   * @param path path to the file containing N-Triples
   */
  def saveAsNTriplesFile(triples: DataSet[Triple], path: String): Unit = {
    import net.sansa_stack.rdf.common.io.ntriples.JenaTripleToNTripleString
    triples
      .map(new JenaTripleToNTripleString()) // map to N-Triples string
      .writeAsText(path)
  }

}
