package net.sansa_stack.inference.spark.data.model

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import net.sansa_stack.inference.data.{RDFTriple, SQLSchema, SQLSchemaDefault}

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFGraphDataFrame(override val triples: DataFrame, val schema: SQLSchema = SQLSchemaDefault)
    extends AbstractRDFGraphSpark[DataFrame, String, RDFTriple, RDFGraphDataFrame](triples) {

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return RDD of triples
    */
  override def find(s: Option[String] = None, p: Option[String] = None, o: Option[String] = None): RDFGraphDataFrame = {
    var sql = s"SELECT ${schema.subjectCol}, ${schema.predicateCol}, ${schema.objectCol} FROM ${schema.triplesTable}"

    // corner case is when nothing is set, i.e. all triples will be returned
    if (s.isDefined || p.isDefined || o.isDefined) {
      sql += " WHERE "

      val conditions = scala.collection.mutable.ListBuffer[String]()

      if (s.isDefined) conditions += s"${schema.subjectCol} = '${s.get}'"
      if (p.isDefined) conditions += s"${schema.predicateCol} = '${p.get}'"
      if (o.isDefined) conditions += s"${schema.objectCol} = '${o.get}'"

      sql += conditions.mkString(" AND ")
    }

    new RDFGraphDataFrame(triples.sqlContext.sql(sql))
  }

  /**
    * Return the union of the current RDF graph with the given RDF graph
    *
    * @param graph the other RDF graph
    * @return the union of both graphs
    */
  def union(graph: RDFGraphDataFrame): RDFGraphDataFrame = {
    new RDFGraphDataFrame(triples.union(graph.toDataFrame()))
  }

  def unionAll(graphs: Seq[RDFGraphDataFrame]): RDFGraphDataFrame = {
    // the Dataframe based solution
    //        return graphs.reduce(_ union _)

    // to limit the lineage, we convert to RDDs first, and use the SparkContext Union method for a sequence of RDDs
    val df: Option[DataFrame] = graphs match {
      case g :: Nil => Some(g.toDataFrame())
      case g :: _ =>
        Some(
          g.toDataFrame()
            .sqlContext
            .createDataFrame(
              g.toDataFrame().sqlContext.sparkContext.union(graphs.map(_.toDataFrame().rdd)),
              g.toDataFrame().schema
            )
        )
      case _ => None
    }
    new RDFGraphDataFrame(df.get)
  }

  def distinct(): RDFGraphDataFrame = {
    new RDFGraphDataFrame(triples.distinct())
  }

  /**
    * Return the number of triples.
    *
    * @return the number of triples
    */
  def size(): Long = {
    triples.count()
  }

  def toDataFrame(sparkSession: SparkSession, schema: SQLSchema = SQLSchemaDefault): DataFrame = triples

  def toRDD(): RDD[RDFTriple] = triples.rdd.map(row => RDFTriple(row.getString(0), row.getString(1), row.getString(2)))

  def cache(): this.type = {
    triples.cache()
    this
  }

  /**
    * Returns a new RDF graph that contains only triples matching the given input.
    *
    * @return a new RDF graph
    */
  override def find(triple: RDFTriple): RDFGraphDataFrame = find(
    if (triple.s.startsWith("?")) None else Some(triple.s),
    if (triple.p.startsWith("?")) None else Some(triple.p),
    if (triple.o.startsWith("?")) None else Some(triple.o)
  )

  /**
    * Returns a new RDF graph that contains the intersection of the current RDF graph with the given RDF graph.
    *
    * @param graph the other RDF graph
    * @return the intersection of both RDF graphs
    */
  override def intersection(graph: RDFGraphDataFrame): RDFGraphDataFrame =
    new RDFGraphDataFrame(this.triples.intersect(graph.triples))

  /**
    * Returns a new RDF graph that contains the difference between the current RDF graph and the given RDF graph.
    *
    * @param graph the other RDF graph
    * @return the difference of both RDF graphs
    */
  override def difference(graph: RDFGraphDataFrame): RDFGraphDataFrame =
    new RDFGraphDataFrame(this.triples.except(graph.triples))
}
