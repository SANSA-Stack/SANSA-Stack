package net.sansa_stack.ml.spark.mining.amieSpark

import org.apache.jena.graph.Triple
import org.apache.spark.sql.{DataFrame, SparkSession}
import net.sansa_stack.ml.spark.mining.amieSpark._
/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFGraphDataFrame(triples: DataFrame) extends AbstractRDFGraph[DataFrame, RDFGraphDataFrame](triples){

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return RDD of triples
    */
  override def find (s: Option[String] = None, p: Option[String] = None, o: Option[String] = None): DataFrame = {
    var sql = "SELECT subject, predicate, object FROM TRIPLES"

    // corner case is when nothing is set, i.e. all triples will be returned
    if(s.isDefined || p.isDefined || o.isDefined) {
      sql += " WHERE "

      val conditions = scala.collection.mutable.ListBuffer[String]()

      if(s.isDefined) conditions += "subject = '" + s.get + "'"
      if(p.isDefined) conditions += "predicate = '" + p.get + "'"
      if(o.isDefined) conditions += "object = '" + o.get + "'"

      sql += conditions.mkString(" AND ")
    }

    triples.sqlContext.sql(sql)
  }

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @return RDD of triples
    */
  def find(triple: Triple): DataFrame = {
    find(
      if (triple.getSubject.isVariable) None else Option(triple.getSubject.toString),
      if (triple.getPredicate.isVariable) None else Option(triple.getPredicate.toString),
      if (triple.getObject.isVariable) None else Option(triple.getObject.toString)
    )
  }

  /**
    * Return the union of the current RDF graph with the given RDF graph
 *
    * @param graph the other RDF graph
    * @return the union of both graphs
    */
  def union(graph: RDFGraphDataFrame): RDFGraphDataFrame = {
    new RDFGraphDataFrame(triples.unionAll(graph.toDataFrame()))
  }

  def cache(): this.type = {
    triples.cache()
    this
  }

  def distinct() = {
    new RDFGraphDataFrame(triples.distinct())
  }

  /**
    * Return the number of triples.
 *
    * @return the number of triples
    */
  def size() = {
    triples.count()
  }

  def toDataFrame(sparkSession: SparkSession): DataFrame = triples

  def toRDD() = triples.rdd.map(row => RDFTriple(row.getString(0), row.getString(1), row.getString(2)))
}
