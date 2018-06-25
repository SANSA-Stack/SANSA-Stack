package net.sansa_stack.rdf.spark.model.df

import org.apache.jena.graph.{ NodeFactory, Triple }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StringType, StructField, StructType }

/**
 * Spark/DataFrame based implementation of DataFrame of triples.
 *
 * @author Gezim Sejdiu Lorenz Buehmann
 */

object TripleOps {

  /**
   * Convert a [[DataFrame]] into a RDD[Triple].
   *
   * @param triples DataFrame of triples (as string).
   * @return a RDD of triples.
   */
  def toRDD(triples: DataFrame): RDD[Triple] = {
    triples.rdd.map(row =>
      Triple.create(
        NodeFactory.createURI(row.getString(0)),
        NodeFactory.createURI(row.getString(1)),
        if (row.getString(2).startsWith("http:")) {
          NodeFactory.createURI(row.getString(2))
        } else NodeFactory.createLiteral(row.getString(2))))
  }

  /**
   * Convert a DataFrame of Triple into a Dataset of Triple.
   *
   * @param triples DataFrame of triples.
   * @return a Dataset of triples.
   */
  def toDS(triples: DataFrame): Dataset[Triple] = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    implicit val encoder = Encoders.kryo[Triple]
    // triples.as[Triple]
    spark.createDataset[Triple](toRDD(triples))
  }

  /**
   * Get triples.
   *
   * @param triples DataFrame of triples.
   * @return DataFrame which contains list of the triples.
   */
  def getTriples(triples: DataFrame): DataFrame =
    triples

  /**
   * Get subjects.
   *
   * @param triples DataFrame of triples.
   * @return DataFrame which contains list of the subjects.
   */
  def getSubjects(triples: DataFrame): DataFrame =
    triples.select("s")

  /**
   * Get predicates.
   *
   * @param triples DataFrame of triples.
   * @return DataFrame which contains list of the predicates.
   */
  def getPredicates(triples: DataFrame): DataFrame =
    triples.select("p")

  /**
   * Get objects.
   *
   * @param triples DataFrame of triples.
   * @return DataFrame which contains list of the objects.
   */
  def getObjects(triples: DataFrame): DataFrame =
    triples.select("o")

  /**
   * Returns an DataFrame of triples that match with the given input.
   *
   * @param triples DataFrame of triples
   * @param subject the subject
   * @param predicate the predicate
   * @param object the object
   * @return DataFrame of triples
   */
  def find(triples: DataFrame, subject: Option[String] = None, predicate: Option[String] = None, `object`: Option[String] = None): DataFrame = {

    val sql = getSQL(subject, predicate, `object`)

    triples.sqlContext.sql(sql)
  }

  /**
   * Generate the translated SQL statement from the triple pattern.
   *
   * @param subject the subject
   * @param predicate the predicate
   * @param object the object
   * @return the translated SQL statement as a string
   */
  def getSQL(subject: Option[String] = None, predicate: Option[String] = None, `object`: Option[String] = None): String = {

    var sql = s"SELECT s, p, o FROM TRIPLES"

    if (subject.isDefined || predicate.isDefined || `object`.isDefined) {
      sql += " WHERE "

      val conditions = scala.collection.mutable.ListBuffer[String]()

      if (subject.isDefined) conditions += s"s = '${subject.get}'"
      if (predicate.isDefined) conditions += s"p = '${predicate.get}'"
      if (`object`.isDefined) conditions += s"o = '${`object`.get}'"

      sql += conditions.mkString(" AND ")
    }
    sql
  }

  /**
   * Returns an DataFrame of triples that match with the given input.
   *
   * @param triples DataFrame of triples
   * @param triple the triple to be checked
   * @return DataFrame of triples that match the given input
   */
  def find(triples: DataFrame, triple: Triple): DataFrame = {
    find(
      triples,
      if (triple.getSubject.isVariable) None else Option(triple.getSubject.getURI),
      if (triple.getPredicate.isVariable) None else Option(triple.getPredicate.getURI),
      if (triple.getObject.isVariable) None else {
        Option(if (triple.getObject.isLiteral()) {
          triple.getObject.getLiteralLexicalForm
        } else triple.getObject.getURI)
      })
  }

  /**
   * Return the number of triples.
   *
   * @param triples DataFrame of triples
   * @return the number of triples
   */
  def size(triples: DataFrame): Long =
    triples.count()

  /**
   * Return the union of this RDF graph and another one.
   *
   * @param triples DataFrame of RDF graph
   * @param other the other RDF graph
   * @return graph (union of both)
   */
  def union(triples: DataFrame, other: DataFrame): DataFrame =
    triples.union(other)

  /**
   * Return the union all of RDF graphs.
   *
   * @param triples DataFrame of RDF graph
   * @param others sequence of DataFrames of other RDF graph
   * @return graph (union of all)
   */
  def unionAll(triples: DataFrame, others: Seq[DataFrame]): DataFrame = {
    val df: Option[DataFrame] = others match {
      case g :: Nil => Some(g.toDF())
      case g :: _ =>
        Some(
          g.toDF()
            .sqlContext
            .createDataFrame(
              g.toDF().sqlContext.sparkContext.union(others.map(_.toDF().rdd)),
              g.toDF().schema))
      case _ => None
    }
    df.get
  }

  /**
   * Returns a new RDF graph that contains the intersection
   * of the current RDF graph with the given RDF graph.
   *
   * @param triples DataFrame of RDF graph
   * @param other the other RDF graph
   * @return the intersection of both RDF graphs
   */
  def intersection(triples: DataFrame, other: DataFrame): DataFrame =
    triples.intersect(other)

  /**
   * Returns a new RDF graph that contains the difference
   * between the current RDF graph and the given RDF graph.
   *
   * @param triples DataFrame of RDF graph
   * @param other the other RDF graph
   * @return the difference of both RDF graphs
   */
  def difference(triples: DataFrame, other: DataFrame): DataFrame =
    triples.except(other)

  /**
   * Determine whether this RDF graph contains any triples
   * with a given (subject, predicate, object) pattern.
   *
   * @param triples DataFrame of triples
   * @param subject the subject (None for any)
   * @param predicate the predicate (None for any)
   * @param object the object (None for any)
   * @return true if there exists within this RDF graph
   * a triple with (S, P, O) pattern, false otherwise
   */
  def contains(triples: DataFrame, subject: Option[String] = None, predicate: Option[String] = None, `object`: Option[String] = None): Boolean = {
    find(triples, subject, predicate, `object`).count() > 0
  }

  /**
   * Determine if a triple is present in this RDF graph.
   *
   * @param triples DataFrame of triples
   * @param triple the triple to be checked
   * @return true if the statement s is in this RDF graph, false otherwise
   */
  def contains(triples: DataFrame, triple: Triple): Boolean = {
    find(triples, triple).count() > 0
  }

  /**
   * Determine if any of the triples in an RDF graph are also contained in this RDF graph.
   *
   * @param triples DataFrame of triples
   * @param other the other RDF graph containing the statements to be tested
   * @return true if any of the statements in RDF graph are also contained
   * in this RDF graph and false otherwise.
   */
  def containsAny(triples: DataFrame, other: DataFrame): Boolean = {
    difference(triples, other).count() > 0
  }

  /**
   * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
   *
   * @param triples DataFrame of triples
   * @param other the other RDF graph containing the statements to be tested
   * @return true if all of the statements in RDF graph are also contained
   * in this RDF graph and false otherwise.
   */
  def containsAll(triples: DataFrame, other: DataFrame): Boolean = {
    difference(triples, other).count() == 0
  }

  /**
   * Add a statement to the current RDF graph.
   *
   * @param triples DataFrame of RDF graph
   * @param triple the triple to be added.
   * @return new DataFrame of triples containing this statement.
   */
  def add(triples: DataFrame, triple: Triple): DataFrame = {
    import net.sansa_stack.rdf.spark.model._
    val statement = triples.sparkSession.sparkContext.parallelize(Seq(triple))
    union(triples, statement.toDF())
  }

  /**
   * Add a list of statements to the current RDF graph.
   *
   * @param triples DataFrame of RDF graph
   * @param triple the list of triples to be added.
   * @return new DataFrame of triples containing this list of statements.
   */
  def addAll(triples: DataFrame, triple: Seq[Triple]): DataFrame = {
    import net.sansa_stack.rdf.spark.model._
    val statements = triples.sparkSession.sparkContext.parallelize(triple)
    union(triples, statements.toDF())
  }

  /**
   * Removes a statement from the current RDF graph.
   * The statement with the same subject, predicate and
   * object as that supplied will be removed from the model.
   *
   * @param triples DataFrame of RDF graph
   * @param triple the statement to be removed.
   * @return new DataFrame of triples without this statement.
   */
  def remove(triples: DataFrame, triple: Triple): DataFrame = {
    import net.sansa_stack.rdf.spark.model._
    val statement = triples.sparkSession.sparkContext.parallelize(Seq(triple))
    difference(triples, statement.toDF())
  }

  /**
   * Removes all the statements from the current RDF graph.
   * The statements with the same subject, predicate and
   * object as those supplied will be removed from the model.
   *
   * @param triples DataFrame of RDF graph
   * @param triple the list of statements to be removed.
   * @return new DataFrame of triples without these statements.
   */
  def removeAll(triples: DataFrame, triple: Seq[Triple]): DataFrame = {
    import net.sansa_stack.rdf.spark.model._
    val statements = triples.sparkSession.sparkContext.parallelize(triple)
    difference(triples, statements.toDF())
  }

  /**
   * Write N-Triples from a given DataFrame of triples
   *
   * @param triples DataFrame of RDF graph
   * @param path path to the file containing N-Triples
   */
  def saveAsNTriplesFile(triples: DataFrame, path: String): Unit = {
    import net.sansa_stack.rdf.spark.io.ntriples._
    toRDD(triples)
      .map(new JenaTripleToNTripleString()) // map to N-Triples string
      .saveAsTextFile(path)
  }
}
