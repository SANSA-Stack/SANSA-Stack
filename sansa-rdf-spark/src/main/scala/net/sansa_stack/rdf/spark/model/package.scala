package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Dataset }

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

    import net.sansa_stack.rdf.spark.model.rdd.TripleOps

    /**
     * Convert a [[RDD[Triple]]] into a DataFrame.
     * @return a DataFrame of triples.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.toDF]]
     */
    def toDF(): DataFrame =
      TripleOps.toDF(triples)

    /**
     * Convert an RDD of Triple into a Dataset of Triple.
     * @return a Dataset of triples.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.toDS]]
     */
    def toDS(): Dataset[Triple] =
      TripleOps.toDS(triples)

    /**
     * Get triples.
     *
     * @return [[RDD[Triple]]] which contains list of the triples.
     */
    def getTriples(): RDD[Triple] =
      TripleOps.getTriples(triples)

    /**
     * Get subjects.
     * @return [[RDD[Node]]] which contains list of the subjects.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.getSubjects]]
     */
    def getSubjects(): RDD[Node] =
      TripleOps.getSubjects(triples)

    /**
     * Get predicates.
     * @return [[RDD[Node]]] which contains list of the predicates.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.getPredicates]]
     */
    def getPredicates(): RDD[Node] =
      TripleOps.getPredicates(triples)

    /**
     * Get objects.
     * @return [[RDD[Node]]] which contains list of the objects.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.getObjects]]
     */
    def getObjects(): RDD[Node] =
      TripleOps.getObjects(triples)

    /**
     * Filter out the subject from a given RDD[Triple],
     * based on a specific function @func .
     * @param func a partial funtion.
     * @return [[RDD[Triple]]] a subset of the given RDD.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.filterSubjects]]
     */
    def filterSubjects(func: Node => Boolean): RDD[Triple] =
      TripleOps.filterSubjects(triples, func)

    /**
     * Filter out the predicates from a given RDD[Triple],
     * based on a specific function @func .
     * @param func a partial funtion.
     * @return [[RDD[Triple]]] a subset of the given RDD.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.filterPredicates]]
     */
    def filterPredicates(func: Node => Boolean): RDD[Triple] =
      TripleOps.filterPredicates(triples, func)

    /**
     * Filter out the objects from a given RDD[Triple],
     * based on a specific function @func .
     * @param func a partial funtion.
     * @return [[RDD[Triple]]] a subset of the given RDD.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.filterObjects]]
     */
    def filterObjects(func: Node => Boolean): RDD[Triple] =
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
    def find(subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): RDD[Triple] =
      TripleOps.find(triples, subject, predicate, `object`)

    /**
     * Returns an RDD of triples that match with the given input.
     *
     * @param triple the triple to be checked
     * @return RDD of triples that match the given input
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.find]]
     */
    def find(triple: Triple): RDD[Triple] =
      TripleOps.find(triples, triple)

    /**
     * Determine whether this RDF graph contains any triples
     * with a given subject and predicate.
     *
     * @param subject the subject (None for any)
     * @param predicate the predicate (Node for any)
     * @return true if there exists within this RDF graph
     * a triple with subject and predicate, false otherwise
     */
    def contains(subject: Some[Node], predicate: Some[Node]): Boolean =
      TripleOps.contains(triples, subject, predicate, None)

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
    def contains(subject: Some[Node], predicate: Some[Node], `object`: Some[Node]): Boolean =
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
    def containsAny(other: RDD[Triple]): Boolean =
      TripleOps.containsAny(triples, other)

    /**
     * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
     *
     * @param other the other RDF graph containing the statements to be tested
     * @return true if all of the statements in RDF graph are also contained
     * in this RDF graph and false otherwise.
     */
    def containsAll(other: RDD[Triple]): Boolean =
      TripleOps.containsAll(triples, other)

    /**
     * Return the number of triples.
     * @return the number of triples
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.size]]
     */
    def size(): Long =
      TripleOps.size(triples)

    /**
     * Return the union of this RDF graph and another one.
     *
     * @param other of the other RDF graph
     * @return graph (union of both)
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.union]]
     */
    def union(other: RDD[Triple]): RDD[Triple] =
      TripleOps.union(triples, other)

    /**
     * Return the union all of RDF graphs.
     *
     * @return graph (union of all)
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.unionAll]]
     */
    def unionAll(others: Seq[RDD[Triple]]): RDD[Triple] =
      TripleOps.unionAll(triples, others)

    /**
     * Returns a new RDF graph that contains the intersection of the current RDF graph with the given RDF graph.
     *
     * @param other of the other RDF graph
     * @return the intersection of both RDF graphs
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.intersection]]
     */
    def intersection(other: RDD[Triple]): RDD[Triple] =
      TripleOps.intersection(triples, other)
    /**
     * Returns a new RDF graph that contains the difference between the current RDF graph and the given RDF graph.
     *
     * @param other of the other RDF graph
     * @return the difference of both RDF graphs
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.difference]]
     */
    def difference(other: RDD[Triple]): RDD[Triple] =
      TripleOps.difference(triples, other)

    /**
     * Add a statement to the current RDF graph.
     *
     * @param triple the triple to be added.
     * @return new RDD of triples containing this statement.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.add]]
     */
    def add(triple: Triple): RDD[Triple] =
      TripleOps.add(triples, triple)

    /**
     * Add a list of statements to the current RDF graph.
     *
     * @param triple the list of triples to be added.
     * @return new RDD of triples containing this list of statements.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.addAll]]
     */
    def addAll(triple: Seq[Triple]): RDD[Triple] =
      TripleOps.addAll(triples, triple)

    /**
     * Removes a statement from the current RDF graph.
     * The statement with the same subject, predicate and object as that supplied will be removed from the model.
     * @param triple the statement to be removed.
     * @return new RDD of triples without this statement.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.remove]]
     */
    def remove(triple: Triple): RDD[Triple] =
      TripleOps.remove(triples, triple)

    /**
     * Removes all the statements from the current RDF graph.
     * The statements with the same subject, predicate and object as those supplied will be removed from the model.
     * @param triple the list of statements to be removed.
     * @return new RDD of triples without these statements.
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.removeAll]]
     */
    def removeAll(triple: Seq[Triple]): RDD[Triple] =
      TripleOps.removeAll(triples, triple)

    /**
     * Write N-Triples from a given RDD of triples
     *
     * @param path path to the file containing N-Triples
     * @see [[net.sansa_stack.rdf.spark.model.rdd.TripleOps.saveAsNTriplesFile]]
     */
    def saveAsNTriplesFile(path: String): Unit =
      TripleOps.saveAsNTriplesFile(triples, path)

  }

  /**
   * Adds all methods to [[DataFrame]] that allows to use TripleOps functions.
   */
  implicit class DFTripleOperations(triples: DataFrame) extends Logging {

    import net.sansa_stack.rdf.spark.model.df.TripleOps

    /**
     * Convert a DataFrame of triples into [[RDD[Triple]]].
     * @return a DataFrame of triples.
     */
    def toRDD(): RDD[Triple] =
      TripleOps.toRDD(triples)

    /**
     * Convert an DataFrame of triples into a Dataset of Triple.
     * @return a Dataset of triples.
     */
    def toDS(): Dataset[Triple] =
      TripleOps.toDS(triples)

    /**
     * Get triples.
     *
     * @return [[RDD[Triple]]] which contains list of the triples.
     */
    def getTriples(): DataFrame =
      TripleOps.getTriples(triples)

    /**
     * Get subjects.
     *
     * @return DataFrame which contains list of the subjects.
     */
    def getSubjects(): DataFrame =
      TripleOps.getSubjects(triples)

    /**
     * Get predicates.
     *
     * @return DataFrame which contains list of the predicates.
     */
    def getPredicates(): DataFrame =
      TripleOps.getPredicates(triples)

    /**
     * Get objects.
     *
     * @return DataFrame which contains list of the objects.
     */
    def getObjects(): DataFrame =
      TripleOps.getObjects(triples)

    /**
     * Returns an DataFrame of triples that match with the given input.
     *
     * @param subject the subject
     * @param predicate the predicate
     * @param object the object
     * @return DataFrame of triples
     */
    def find(subject: Option[String] = None, predicate: Option[String] = None, `object`: Option[String] = None): DataFrame =
      TripleOps.find(triples, subject, predicate, `object`)

    /**
     * Returns an DataFrame of triples that match with the given input.
     *
     * @param triple the triple to be checked
     * @return DataFrame of triples that match the given input
     */
    def find(triple: Triple): DataFrame =
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
     * @param triples DataFrame of RDF graph
     * @param other the other RDF graph
     * @return graph (union of both)
     */
    def union(other: DataFrame): DataFrame =
      TripleOps.union(triples, other)

    /**
     * Return the union all of RDF graphs.
     *
     * @param others sequence of DataFrames of other RDF graph
     * @return graph (union of all)
     */
    def unionAll(others: Seq[DataFrame]): DataFrame =
      TripleOps.unionAll(triples, others)

    /**
     * Returns a new RDF graph that contains the intersection
     * of the current RDF graph with the given RDF graph.
     *
     * @param other the other RDF graph
     * @return the intersection of both RDF graphs
     */
    def intersection(other: DataFrame): DataFrame =
      TripleOps.intersection(triples, other)

    /**
     * Returns a new RDF graph that contains the difference
     * between the current RDF graph and the given RDF graph.
     *
     * @param other the other RDF graph
     * @return the difference of both RDF graphs
     */
    def difference(other: DataFrame): DataFrame =
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
    def contains(subject: Option[String] = None, predicate: Option[String] = None, `object`: Option[String] = None): Boolean =
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
    def containsAny(other: DataFrame): Boolean =
      TripleOps.containsAny(triples, other)

    /**
     * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
     *
     * @param other the other RDF graph containing the statements to be tested
     * @return true if all of the statements in RDF graph are also contained
     * in this RDF graph and false otherwise.
     */
    def containsAll(other: DataFrame): Boolean =
      TripleOps.containsAll(triples, other)

    /**
     * Add a statement to the current RDF graph.
     *
     * @param triple the triple to be added.
     * @return new DataFrame of triples containing this statement.
     */
    def add(triple: Triple): DataFrame =
      TripleOps.add(triples, triple)

    /**
     * Add a list of statements to the current RDF graph.
     *
     * @param triple the list of triples to be added.
     * @return new DataFrame of triples containing this list of statements.
     */
    def addAll(triple: Seq[Triple]): DataFrame =
      TripleOps.addAll(triples, triple)

    /**
     * Removes a statement from the current RDF graph.
     * The statement with the same subject, predicate and
     * object as that supplied will be removed from the model.
     *
     * @param triple the statement to be removed.
     * @return new DataFrame of triples without this statement.
     */
    def remove(triple: Triple): DataFrame =
      TripleOps.remove(triples, triple)

    /**
     * Removes all the statements from the current RDF graph.
     * The statements with the same subject, predicate and
     * object as those supplied will be removed from the model.
     *
     * @param triple the list of statements to be removed.
     * @return new DataFrame of triples without these statements.
     */
    def removeAll(triple: Seq[Triple]): DataFrame =
      TripleOps.removeAll(triples, triple)

    /**
     * Write N-Triples from a given DataFrame of triples
     *
     * @param triples DataFrame of RDF graph
     * @param path path to the file containing N-Triples
     */
    def saveAsNTriplesFile(path: String): Unit =
      TripleOps.saveAsNTriplesFile(triples, path)
  }

  /**
   * Adds all methods to [[Dataset[Triple]]] that allows to use TripleOps functions.
   */
  implicit class DSTripleOperations(triples: Dataset[Triple]) extends Logging {

    import net.sansa_stack.rdf.spark.model.ds.TripleOps

    /**
     * Convert a Dataset of triples into [[RDD[Triple]]].
     * @return a RDD of triples.
     */
    def toRDD(): RDD[Triple] =
      TripleOps.toRDD(triples)

    /**
     * Convert an Dataset of triples into a DataFrame of triples.
     * @return a DataFrame of triples.
     */
    def toDF(): DataFrame =
      TripleOps.toDF(triples)

    /**
     * Get triples.
     *
     * @return [[Dataset[Triple]]] which contains list of the triples.
     */
    def getTriples(): Dataset[Triple] =
      TripleOps.getTriples(triples)

    /**
     * Returns an Dataset of triples that match with the given input.
     *
     * @param subject the subject
     * @param predicate the predicate
     * @param object the object
     * @return Dataset of triples
     */
    def find(subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): Dataset[Triple] =
      TripleOps.find(triples, subject, predicate, `object`)

    /**
     * Returns an Dataset of triples that match with the given input.
     *
     * @param triple the triple to be checked
     * @return Dataset of triples that match the given input
     */
    def find(triple: Triple): Dataset[Triple] =
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
     * @param triples Dataset of RDF graph
     * @param other the other RDF graph
     * @return graph (union of both)
     */
    def union(other: Dataset[Triple]): Dataset[Triple] =
      TripleOps.union(triples, other)

    /**
     * Return the union all of RDF graphs.
     *
     * @param others sequence of Dataset of other RDF graph
     * @return graph (union of all)
     */
    def unionAll(others: Seq[Dataset[Triple]]): Dataset[Triple] =
      TripleOps.unionAll(triples, others)

    /**
     * Returns a new RDF graph that contains the intersection
     * of the current RDF graph with the given RDF graph.
     *
     * @param other the other RDF graph
     * @return the intersection of both RDF graphs
     */
    def intersection(other: Dataset[Triple]): Dataset[Triple] =
      TripleOps.intersection(triples, other)

    /**
     * Returns a new RDF graph that contains the difference
     * between the current RDF graph and the given RDF graph.
     *
     * @param other the other RDF graph
     * @return the difference of both RDF graphs
     */
    def difference(other: Dataset[Triple]): Dataset[Triple] =
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
    def containsAny(other: Dataset[Triple]): Boolean =
      TripleOps.containsAny(triples, other)

    /**
     * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
     *
     * @param other the other RDF graph containing the statements to be tested
     * @return true if all of the statements in RDF graph are also contained
     * in this RDF graph and false otherwise.
     */
    def containsAll(other: Dataset[Triple]): Boolean =
      TripleOps.containsAll(triples, other)

    /**
     * Add a statement to the current RDF graph.
     *
     * @param triple the triple to be added.
     * @return new Dataset of triples containing this statement.
     */
    def add(triple: Triple): Dataset[Triple] =
      TripleOps.add(triples, triple)

    /**
     * Add a list of statements to the current RDF graph.
     *
     * @param triple the list of triples to be added.
     * @return new Dataset of triples containing this list of statements.
     */
    def addAll(triple: Seq[Triple]): Dataset[Triple] =
      TripleOps.addAll(triples, triple)

    /**
     * Removes a statement from the current RDF graph.
     * The statement with the same subject, predicate and
     * object as that supplied will be removed from the model.
     *
     * @param triple the statement to be removed.
     * @return new Dataset of triples without this statement.
     */
    def remove(triple: Triple): Dataset[Triple] =
      TripleOps.remove(triples, triple)

    /**
     * Removes all the statements from the current RDF graph.
     * The statements with the same subject, predicate and
     * object as those supplied will be removed from the model.
     *
     * @param triple the list of statements to be removed.
     * @return new Dataset of triples without these statements.
     */
    def removeAll(triple: Seq[Triple]): Dataset[Triple] =
      TripleOps.removeAll(triples, triple)

    /**
     * Write N-Triples from a given Dataset of triples
     *
     * @param triples Dataset of RDF graph
     * @param path path to the file containing N-Triples
     */
    def saveAsNTriplesFile(path: String): Unit =
      TripleOps.saveAsNTriplesFile(triples, path)
  }

}
