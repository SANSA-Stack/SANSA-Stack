package net.sansa_stack.rdf.spark.model.rdd

import net.sansa_stack.rdf.spark.utils._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StringType, StructField, StructType }

/**
 * Spark/RDD based implementation of RDD[Triple].
 *
 * @author Gezim Sejdiu
 */
object TripleOps {

  /**
   * Convert a [[RDD[Triple]]] into a DataFrame.
   *
   * @param triples RDD of triples.
   * @return a DataFrame of triples.
   */
  def toDF(triples: RDD[Triple]): DataFrame = {

    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val schema = SchemaUtils.SQLSchemaDefault

    val rowRDD = triples.map(t =>
      Row(
        NodeUtils.getNodeValue(t.getSubject),
        NodeUtils.getNodeValue(t.getPredicate),
        NodeUtils.getNodeValue(t.getObject)))
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("TRIPLES")
    df
  }

  /**
   * Convert an RDD of Triple into a Dataset of Triple.
   *
   * @param triples RDD of triples.
   * @return a Dataset of triples.
   */
  def toDS(triples: RDD[Triple]): Dataset[Triple] = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    implicit val encoder = Encoders.kryo[Triple]
    spark.createDataset[Triple](triples)
  }

  /**
   * Get triples.
   *
   * @param triples RDD of triples.
   * @return [[RDD[Triple]]] which contains list of the triples.
   */
  def getTriples(triples: RDD[Triple]): RDD[Triple] =
    triples

  /**
   * Get subjects.
   *
   * @param triples RDD of triples.
   * @return [[RDD[Node]]] which contains list of the subjects.
   */
  def getSubjects(triples: RDD[Triple]): RDD[Node] =
    triples.map(_.getSubject)

  /**
   * Get predicates.
   *
   * @param triples RDD of triples.
   * @return [[RDD[Node]]] which contains list of the predicates.
   */
  def getPredicates(triples: RDD[Triple]): RDD[Node] =
    triples.map(_.getPredicate)

  /**
   * Get objects.
   *
   * @param triples RDD of triples.
   * @return [[RDD[Node]]] which contains list of the objects.
   */
  def getObjects(triples: RDD[Triple]): RDD[Node] =
    triples.map(_.getObject)

  /**
   * Filter out the subject from a given RDD[Triple],
   * based on a specific function @func .
   *
   * @param triples RDD of triples.
   * @param func a partial funtion.
   * @return [[RDD[Triple]]] a subset of the given RDD.
   */
  def filterSubjects(triples: RDD[Triple], func: Node => Boolean): RDD[Triple] =
    triples.filter(f => func(f.getSubject))

  /**
   * Filter out the predicates from a given RDD[Triple],
   * based on a specific function @func .
   *
   * @param triples RDD of triples.
   * @param func a partial funtion.
   * @return [[RDD[Triple]]] a subset of the given RDD.
   */
  def filterPredicates(triples: RDD[Triple], func: Node => Boolean): RDD[Triple] =
    triples.filter(f => func(f.getPredicate))

  /**
   * Filter out the objects from a given RDD[Triple],
   * based on a specific function @func .
   *
   * @param triples RDD of triples.
   * @param func a partial funtion.
   * @return [[RDD[Triple]]] a subset of the given RDD.
   */
  def filterObjects(triples: RDD[Triple], func: Node => Boolean): RDD[Triple] =
    triples.filter(f => func(f.getSubject))

  /**
   * Returns an RDD of triples that match with the given input.
   *
   * @param triples RDD of triples
   * @param subject the subject
   * @param predicate the predicate
   * @param object the object
   * @return RDD of triples
   */
  def find(triples: RDD[Triple], subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): RDD[Triple] = {
    triples.filter(t =>
      (subject == None || t.getSubject.matches(subject.get)) &&
        (predicate == None || t.getPredicate.matches(predicate.get)) &&
        (`object` == None || t.getObject.matches(`object`.get)))
  }

  /**
   * Returns an RDD of triples that match with the given input.
   *
   * @param triples RDD of triples
   * @param triple  the triple to be checked
   * @return RDD of triples that match the given input
   */
  def find(triples: RDD[Triple], triple: Triple): RDD[Triple] = {
    find(
      triples,
      if (triple.getSubject.isVariable) None else Option(triple.getSubject),
      if (triple.getPredicate.isVariable) None else Option(triple.getPredicate),
      if (triple.getObject.isVariable) None else Option(triple.getObject))
  }

  /**
   * Return the number of triples.
   *
   * @param triples RDD of triples
   * @return the number of triples
   */
  def size(triples: RDD[Triple]): Long =
    triples.count()

  /**
   * Return the union of this RDF graph and another one.
   *
   * @param triples RDD of RDF graph
   * @param other the other RDF graph
   * @return graph (union of both)
   */
  def union(triples: RDD[Triple], other: RDD[Triple]): RDD[Triple] =
    triples.union(other)

  /**
   * Return the union all of RDF graphs.
   *
   * @param triples RDD of RDF graph
   * @param others sequence of RDDs of other RDF graph
   * @return graph (union of all)
   */
  def unionAll(triples: RDD[Triple], others: Seq[RDD[Triple]]): RDD[Triple] = {
    val first = others.head
    first.sparkContext.union(triples)
  }

  /**
   * Returns a new RDF graph that contains the intersection
   * of the current RDF graph with the given RDF graph.
   *
   * @param triples RDD of RDF graph
   * @param other the other RDF graph
   * @return the intersection of both RDF graphs
   */
  def intersection(triples: RDD[Triple], other: RDD[Triple]): RDD[Triple] =
    triples.intersection(other)

  /**
   * Returns a new RDF graph that contains the difference
   * between the current RDF graph and the given RDF graph.
   *
   * @param triples RDD of RDF graph
   * @param other the other RDF graph
   * @return the difference of both RDF graphs
   */
  def difference(triples: RDD[Triple], other: RDD[Triple]): RDD[Triple] =
    triples.subtract(other)

  /**
   * Determine whether this RDF graph contains any triples
   * with a given (subject, predicate, object) pattern.
   *
   * @param triples RDD of triples
   * @param subject the subject (None for any)
   * @param predicate the predicate (None for any)
   * @param object the object (None for any)
   * @return true if there exists within this RDF graph
   * a triple with (S, P, O) pattern, false otherwise
   */
  def contains(triples: RDD[Triple], subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): Boolean = {
    find(triples, subject, predicate, `object`).count() > 0
  }

  /**
   * Determine if a triple is present in this RDF graph.
   *
   * @param triples RDD of triples
   * @param triple the triple to be checked
   * @return true if the statement s is in this RDF graph, false otherwise
   */
  def contains(triples: RDD[Triple], triple: Triple): Boolean = {
    find(triples, triple).count() > 0
  }

  /**
   * Determine if any of the triples in an RDF graph are also contained in this RDF graph.
   *
   * @param triples RDD of triples
   * @param other the other RDF graph containing the statements to be tested
   * @return true if any of the statements in RDF graph are also contained
   * in this RDF graph and false otherwise.
   */
  def containsAny(triples: RDD[Triple], other: RDD[Triple]): Boolean = {
    difference(triples, other).count() > 0
  }

  /**
   * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
   *
   * @param triples RDD of triples
   * @param other the other RDF graph containing the statements to be tested
   * @return true if all of the statements in RDF graph are also contained
   * in this RDF graph and false otherwise.
   */
  def containsAll(triples: RDD[Triple], other: RDD[Triple]): Boolean = {
    difference(triples, other).count() == 0
  }

  @transient var spark: SparkSession = SparkSession.builder.getOrCreate()

  /**
   * Add a statement to the current RDF graph.
   *
   * @param triples RDD of RDF graph
   * @param triple the triple to be added.
   * @return new RDD of triples containing this statement.
   */
  def add(triples: RDD[Triple], triple: Triple): RDD[Triple] = {
    val statement = spark.sparkContext.parallelize(Seq(triple))
    union(triples, statement)
  }

  /**
   * Add a list of statements to the current RDF graph.
   *
   * @param triples RDD of RDF graph
   * @param triple the list of triples to be added.
   * @return new RDD of triples containing this list of statements.
   */
  def addAll(triples: RDD[Triple], triple: Seq[Triple]): RDD[Triple] = {
    val statements = spark.sparkContext.parallelize(triple)
    union(triples, statements)
  }

  /**
   * Removes a statement from the current RDF graph.
   * The statement with the same subject, predicate and
   * object as that supplied will be removed from the model.
   *
   * @param triples RDD of RDF graph
   * @param triple the statement to be removed.
   * @return new RDD of triples without this statement.
   */
  def remove(triples: RDD[Triple], triple: Triple): RDD[Triple] = {
    val statement = spark.sparkContext.parallelize(Seq(triple))
    difference(triples, statement)
  }

  /**
   * Removes all the statements from the current RDF graph.
   * The statements with the same subject, predicate and
   * object as those supplied will be removed from the model.
   *
   * @param triples RDD of RDF graph
   * @param triple the list of statements to be removed.
   * @return new RDD of triples without these statements.
   */
  def removeAll(triples: RDD[Triple], triple: Seq[Triple]): RDD[Triple] = {
    val statements = spark.sparkContext.parallelize(triple)
    difference(triples, statements)
  }

  /**
   * Write N-Triples from a given RDD of triples
   *
   * @param triples RDD of RDF graph
   * @param path path to the file containing N-Triples
   */
  def saveAsNTriplesFile(triples: RDD[Triple], path: String): Unit = {
    import net.sansa_stack.rdf.spark.io.ntriples._
    triples
      .map(new JenaTripleToNTripleString()) // map to N-Triples string
      .saveAsTextFile(path)
  }
}
