package net.sansa_stack.rdf.spark.rdd.op

import net.sansa_stack.rdf.common.io.ntriples.JenaTripleToNTripleString
import net.sansa_stack.rdf.spark.utils.{NodeUtils, SchemaUtils, SparkSessionUtils}
import org.apache.jena.graph.{Graph, Node, Triple}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * Spark/RDD based implementation of RDD[Triple].
 *
 * @author Gezim Sejdiu
 */
object RddOfTriplesOps {

  /**
   * Convert a [[RDD[Triple]]] into a DataFrame of having three string columns.
   *
   * @param triples RDD of triples.
   * @return a DataFrame of triples.
   */
  def toDF(triples: RDD[Triple]): DataFrame = {

    val spark: SparkSession = SparkSessionUtils.getSessionFromRdd(triples)
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
    val spark: SparkSession = SparkSessionUtils.getSessionFromRdd(triples)
    implicit val encoder: Encoder[Triple] = Encoders.kryo[Triple]
    spark.createDataset[Triple](triples)
  }

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
   * @param func    a partial funtion.
   * @return [[RDD[Triple]]] a subset of the given RDD.
   */
  def filterSubjects(triples: RDD[Triple], func: Node => Boolean): RDD[Triple] =
    triples.filter(f => func(f.getSubject))

  /**
   * Filter out the predicates from a given RDD[Triple],
   * based on a specific function @func .
   *
   * @param triples RDD of triples.
   * @param func    a partial funtion.
   * @return [[RDD[Triple]]] a subset of the given RDD.
   */
  def filterPredicates(triples: RDD[Triple], func: Node => Boolean): RDD[Triple] =
    triples.filter(f => func(f.getPredicate))

  // def filterPredicates(triples: RDD[Triple], predicateIris: String*): RDD[Triple] =
  //  filterPredicates(triples, predicateIris.toSet)

  def filterPredicates(triples: RDD[Triple], predicateIris: Set[String]): RDD[Triple] =
    triples.filter(f => predicateIris.contains(f.getPredicate.getURI))

  /**
   * Filter out the objects from a given RDD[Triple],
   * based on a specific function @func .
   *
   * @param triples RDD of triples.
   * @param func    a partial funtion.
   * @return [[RDD[Triple]]] a subset of the given RDD.
   */
  def filterObjects(triples: RDD[Triple], func: Node => Boolean): RDD[Triple] =
    triples.filter(f => func(f.getSubject))

  /**
   * Returns an RDD of triples that match with the given input.
   *
   * @param triples   RDD of triples
   * @param subject   the subject
   * @param predicate the predicate
   * @param object    the object
   * @return RDD of triples
   */
  def find(triples: RDD[Triple], subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): RDD[Triple] = {
    triples.filter(t =>
      (subject.isEmpty || t.getSubject.matches(subject.get)) &&
        (predicate.isEmpty || t.getPredicate.matches(predicate.get)) &&
        (`object`.isEmpty || t.getObject.matches(`object`.get)))
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
   * Return the union all of RDF graphs.
   *
   * @param triples RDD of RDF graph
   * @param others  sequence of RDDs of other RDF graph
   * @return graph (union of all)
   */
  def unionAll(triples: RDD[Triple], others: Seq[RDD[Triple]]): RDD[Triple] = {
    val first = others.head
    first.sparkContext.union(triples)
  }

  /**
   * Determine whether this RDF graph contains any triples
   * with a given (subject, predicate, object) pattern.
   *
   * @param triples   RDD of triples
   * @param subject   the subject (None for any)
   * @param predicate the predicate (None for any)
   * @param object    the object (None for any)
   * @return true if there exists within this RDF graph
   *         a triple with (S, P, O) pattern, false otherwise
   */
  def contains(triples: RDD[Triple], subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): Boolean = {
    find(triples, subject, predicate, `object`).count() > 0
  }

  /**
   * Determine if a triple is present in this RDF graph.
   *
   * @param triples RDD of triples
   * @param triple  the triple to be checked
   * @return true if the statement s is in this RDF graph, false otherwise
   */
  def contains(triples: RDD[Triple], triple: Triple): Boolean = {
    find(triples, triple).count() > 0
  }

  /**
   * Determine if any of the triples in RDF graph `g2` are contained in RDF graph `g1`.
   *
   * @param g1 RDD of triples
   * @param g2 the other RDF graph containing the statements to be tested
   * @return `true` if any of the triples in RDF graph `g2` are contained in RDF graph `g1`, otherwise `false`
   */
  def containsAny(g1: RDD[Triple], g2: RDD[Triple]): Boolean = {
    g1.intersection(g2).count() > 0
  }

  /**
   * Determine if all of the statements in RDF graph `g2` are contained in RDF graph `g1`.
   *
   * @param g1 RDD of triples
   * @param g2 the other RDF graph containing the statements to be tested
   * @return `true`` if all of the statements in RDF graph `g2` are contained in RDF graph `g1`, otherwise `false`
   */
  def containsAll(g1: RDD[Triple], g2: RDD[Triple]): Boolean = {
    g2.subtract(g1).isEmpty()
    // alternative but probably slower: triples.intersection(other).count() == other.count()
  }

  /**
   * Add a statement to the current RDF graph.
   *
   * @param triples RDD of RDF graph
   * @param triple  the triple to be added.
   * @return new RDD of triples containing this statement.
   */
  def add(triples: RDD[Triple], triple: Triple): RDD[Triple] = {
    val spark: SparkSession = SparkSessionUtils.getSessionFromRdd(triples)
    val statement = spark.sparkContext.parallelize(Seq(triple))
    triples.union(statement)
  }

  /**
   * Add a list of statements to the current RDF graph.
   *
   * @param triples RDD of RDF graph
   * @param triple  the list of triples to be added.
   * @return new RDD of triples containing this list of statements.
   */
  def addAll(triples: RDD[Triple], triple: Seq[Triple]): RDD[Triple] = {
    val spark: SparkSession = SparkSessionUtils.getSessionFromRdd(triples)
    val statements = spark.sparkContext.parallelize(triple)
    triples.union(statements)
  }

  /**
   * Removes a statement from the current RDF graph.
   * The statement with the same subject, predicate and
   * object as that supplied will be removed from the model.
   *
   * @param triples RDD of RDF graph
   * @param triple  the statement to be removed.
   * @return new RDD of triples without this statement.
   */
  def remove(triples: RDD[Triple], triple: Triple): RDD[Triple] = {
    val spark: SparkSession = SparkSessionUtils.getSessionFromRdd(triples)
    val statement = spark.sparkContext.parallelize(Seq(triple))
    triples.subtract(statement)
  }

  /**
   * Removes all the statements from the current RDF graph.
   * The statements with the same subject, predicate and
   * object as those supplied will be removed from the model.
   *
   * @param triples RDD of RDF graph
   * @param triple  the list of statements to be removed.
   * @return new RDD of triples without these statements.
   */
  def removeAll(triples: RDD[Triple], triple: Seq[Triple]): RDD[Triple] = {
    val spark: SparkSession = SparkSessionUtils.getSessionFromRdd(triples)
    val statements = spark.sparkContext.parallelize(triple)
    triples.subtract(statements)
  }

  /**
   * Write N-Triples from a given RDD of triples
   *
   * @param triples RDD of RDF graph
   * @param path    path to the file containing N-Triples
   */
  def saveAsNTriplesFile(triples: RDD[Triple], path: String): Unit = {
    triples
      .map(new JenaTripleToNTripleString()) // map to N-Triples string
      .saveAsTextFile(path)
  }


  /**
   * Collect an RDD of triples into a graph
   *
   * @param outGraph the target graph
   * @param triples
   * @return the target graph
   */
  def toGraph(outGraph: Graph, triples: RDD[Triple]): Graph = {
    val col = triples.collect()
    col.foreach(outGraph.add)
    outGraph
  }
}
