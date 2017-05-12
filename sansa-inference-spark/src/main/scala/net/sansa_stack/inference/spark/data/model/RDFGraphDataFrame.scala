package net.sansa_stack.inference.spark.data.model

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import net.sansa_stack.inference.data.{SQLSchema, SQLSchemaDefault}

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFGraphDataFrame(override val triples: DataFrame, val schema: SQLSchema = SQLSchemaDefault)
    extends AbstractRDFGraph[DataFrame, RDFGraphDataFrame](triples) {

  /**
    * Returns an RDD of triples that match with the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return RDD of triples
    */
  override def find(s: Option[Node] = None, p: Option[Node] = None, o: Option[Node] = None): RDFGraphDataFrame = {
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

  def toRDD(): RDD[Triple] = triples.rdd.map(row => Triple.create(row.getString(0), row.getString(1), row.getString(2)))

  def cache(): this.type = {
    triples.cache()
    this
  }
}
