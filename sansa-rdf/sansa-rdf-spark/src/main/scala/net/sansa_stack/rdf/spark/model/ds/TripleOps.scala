package net.sansa_stack.rdf.spark.model.ds

import java.io.ByteArrayOutputStream
import java.util.Collections

import net.sansa_stack.rdf.spark.utils._
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


/**
 * Spark based implementation of Dataset of triples.
 *
 * @author Gezim Sejdiu Lorenz Buehmann
 */
object TripleOps {

  /**
   * Convert a [[Dataset[Triple]] into a RDD[Triple].
   *
   * @param triples Dataset of triples.
   * @return a RDD of triples.
   */
  def toRDD(triples: Dataset[Triple]): RDD[Triple] =
    triples.rdd

  /**
   * Convert a [[Dataset[Triple]]] into a DataFrame.
   *
   * @param triples Dataset of triples.
   * @return a DataFrame of triples.
   */
  def toDF(triples: Dataset[Triple]): DataFrame = {

    val spark: SparkSession = SparkSession.builder().getOrCreate()

    val schema = SchemaUtils.SQLSchemaDefault
    val rowRDD = triples.rdd.map(t =>
      Row(
        NodeUtils.getNodeValue(t.getSubject),
        NodeUtils.getNodeValue(t.getPredicate),
        NodeUtils.getNodeValue(t.getObject)))
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("TRIPLES")
    df
  }

  /**
   * Get triples.
   *
   * @param triples DataFrame of triples.
   * @return DataFrame which contains list of the triples.
   */
  def getTriples(triples: Dataset[Triple]): Dataset[Triple] =
    triples

  /**
   * Returns an Dataset of triples that match with the given input.
   *
   * @param triples Dataset of triples
   * @param subject the subject
   * @param predicate the predicate
   * @param object the object
   * @return Dataset of triples
   */
  def find(triples: Dataset[Triple],
           subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None)
  : Dataset[Triple] = {
    find(triples, Triple.create(subject.getOrElse(Node.ANY), predicate.getOrElse(Node.ANY), `object`.getOrElse(Node.ANY)))
  }

  /**
   * Returns an Dataset of triples that match with the given input.
   *
   * @param triples Dataset of triples
   * @param triple  the triple to be checked
   * @return Dataset of triples that match the given input
   */
  def find(triples: Dataset[Triple], triple: Triple): Dataset[Triple] = {
    triples.filter(triple.matches(_))
  }

  /**
   * Return the union of this RDF graph and another one.
   *
   * @param triples Dataset of RDF graph
   * @param other the other RDF graph
   * @return graph (union of both)
   */
  def union(triples: Dataset[Triple], other: Dataset[Triple]): Dataset[Triple] =
    triples.union(other)

  /**
   * Return the union all of RDF graphs.
   *
   * @param triples Dataset of RDF graph
   * @param others sequence of Datasets of other RDF graph
   * @return graph (union of all)
   */
  def unionAll(triples: Dataset[Triple], others: Seq[Dataset[Triple]]): Dataset[Triple] =
    others.reduce(_ union _)

  /**
   * Returns a new RDF graph that contains the intersection
   * of the current RDF graph with the given RDF graph.
   *
   * @param triples Dataset of RDF graph
   * @param other the other RDF graph
   * @return the intersection of both RDF graphs
   */
  def intersection(triples: Dataset[Triple], other: Dataset[Triple]): Dataset[Triple] =
    triples.intersect(other)

  /**
   * Returns a new RDF graph that contains the difference
   * between the current RDF graph and the given RDF graph.
   *
   * @param triples Dataset of RDF graph
   * @param other the other RDF graph
   * @return the difference of both RDF graphs
   */
  def difference(triples: Dataset[Triple], other: Dataset[Triple]): Dataset[Triple] =
    triples.except(other)

  /**
   * Determine whether this RDF graph contains any triples
   * with a given (subject, predicate, object) pattern.
   *
   * @param triples Dataset of triples
   * @param subject the subject (None for any)
   * @param predicate the predicate (None for any)
   * @param object the object (None for any)
   * @return true if there exists within this RDF graph
   * a triple with (S, P, O) pattern, false otherwise
   */
  def contains(triples: Dataset[Triple], subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): Boolean = {
    contains(triples, Triple.create(subject.getOrElse(Node.ANY), predicate.getOrElse(Node.ANY), `object`.getOrElse(Node.ANY)))
  }

  /**
   * Determine if a triple is present in this RDF graph.
   *
   * @param triples Dataset of triples
   * @param triple the triple to be checked
   * @return true if the triple is in this RDF graph, false otherwise
   */
  def contains(triples: Dataset[Triple], triple: Triple): Boolean = {
//    find(triples, triple).count() > 0
    import triples.sparkSession.implicits._
    !triples.mapPartitions(p => {
      if (p.exists(triple.matches)) Iterator(1) else Iterator()
    }).isEmpty
  }

  /**
   * Determine if any of the triples in an RDF graph are also contained in this RDF graph.
   *
   * @param triples Dataset of triples
   * @param other the other RDF graph containing the statements to be tested
   * @return true if any of the statements in RDF graph are also contained
   * in this RDF graph and false otherwise.
   */
  def containsAny(triples: Dataset[Triple], other: Dataset[Triple]): Boolean = {
    !difference(triples, other).isEmpty
  }

  /**
   * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
   *
   * @param triples Dataset of triples
   * @param other the other RDF graph containing the statements to be tested
   * @return true if all of the statements in RDF graph are also contained
   * in this RDF graph and false otherwise.
   */
  def containsAll(triples: Dataset[Triple], other: Dataset[Triple]): Boolean = {
    difference(triples, other).isEmpty
  }

  @transient var spark: SparkSession = SparkSession.builder().getOrCreate()

  /**
   * Add a statement to the current RDF graph.
   *
   * @param triples Dataset of RDF graph
   * @param triple the triple to be added.
   * @return new Dataset of triples containing this statement.
   */
  def add(triples: Dataset[Triple], triple: Triple): Dataset[Triple] = {
    val statement = spark.sparkContext.parallelize(Seq(triple))
    import net.sansa_stack.rdf.spark.model._
    union(triples, statement.toDS())
  }

  /**
   * Add a list of statements to the current RDF graph.
   *
   * @param triples Dataset of RDF graph
   * @param triple the list of triples to be added.
   * @return new Dataset of triples containing this list of statements.
   */
  def addAll(triples: Dataset[Triple], triple: Seq[Triple]): Dataset[Triple] = {
    val statements = spark.sparkContext.parallelize(triple)
    import net.sansa_stack.rdf.spark.model._
    union(triples, statements.toDS())
  }

  /**
   * Removes a statement from the current RDF graph.
   * The statement with the same subject, predicate and
   * object as that supplied will be removed from the model.
   *
   * @param triples Dataset of RDF graph
   * @param triple the statement to be removed.
   * @return new Dataset of triples without this statement.
   */
  def remove(triples: Dataset[Triple], triple: Triple): Dataset[Triple] = {
    val statement = spark.sparkContext.parallelize(Seq(triple))
    import net.sansa_stack.rdf.spark.model._
    difference(triples, statement.toDS())
  }

  /**
   * Removes all the statements from the current RDF graph.
   * The statements with the same subject, predicate and
   * object as those supplied will be removed from the model.
   *
   * @param triples Dataset of RDF graph
   * @param triple the list of statements to be removed.
   * @return new Dataset of triples without these statements.
   */
  def removeAll(triples: Dataset[Triple], triple: Seq[Triple]): Dataset[Triple] = {
    val statements = spark.sparkContext.parallelize(triple)
    import net.sansa_stack.rdf.spark.model._
    difference(triples, statements.toDS())
  }

  /**
   * Write N-Triples from a given Dataset of triples
   *
   * @param triples Dataset of RDF graph
   * @param path path to the file containing N-Triples
   */
  def saveAsNTriplesFile(triples: Dataset[Triple], path: String): Unit = {
    import triples.sparkSession.implicits._

    import scala.collection.JavaConverters._
    triples.mapPartitions(p => {
      // check if partition is empty
      if (p.hasNext) {
        val os = new ByteArrayOutputStream()
        RDFDataMgr.writeTriples(os, p.asJava)
        Collections.singleton(os.toString("UTF-8").trim).iterator().asScala
      } else {
        Iterator()
      }
    })
      .write.format("text").save(path)
  }
}
