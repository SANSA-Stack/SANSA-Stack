package net.sansa_stack.inference.spark.data

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import net.sansa_stack.inference.data.RDFTriple

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFGraphDataset(override val triples: Dataset[RDFTriple]) extends AbstractRDFGraph[Dataset[RDFTriple], RDFGraphDataset](triples) {

  /**
    * Returns a Dataset of triples that match with the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return Dataset of triples
    */
  override def find(s: Option[String] = None, p: Option[String] = None, o: Option[String] = None): RDFGraphDataset = {
    var result = triples

    if (s.isDefined) result = triples.filter(triples("s") === s.get)
    if (p.isDefined) result = triples.filter(triples("p") === p.get)
    if (o.isDefined) result = triples.filter(triples("o") === o.get)

    new RDFGraphDataset(result)
  }

  /**
    * Returns a Dataset of triples that match with the given input.
    *
    * @return the matching triples
    */
  def find(triple: Triple): RDFGraphDataset = {
    find(
      if (triple.getSubject.isVariable) None else Option(triple.getSubject.toString),
      if (triple.getPredicate.isVariable) None else Option(triple.getPredicate.toString),
      if (triple.getObject.isVariable) None else Option(triple.getObject.toString)
    )
  }

  /**
    * Returns a new RDF graph that comprises the union of the current RDF graph with the given RDF graph
 *
    * @param graph the other RDF graph
    * @return the union of both graphs
    */
  def union(graph: RDFGraphDataset): RDFGraphDataset = {
    new RDFGraphDataset(triples.union(graph.triples))
  }

  def cache(): this.type = {
    triples.cache()
    this
  }

  def distinct(): RDFGraphDataset = {
    new RDFGraphDataset(triples.distinct())
  }

  /**
    * Return the number of triples.
    *
    * @return the number of triples
    */
  def size(): Long = {
    triples.count()
  }

  def toDataFrame(sparkSession: SparkSession): DataFrame = triples.toDF()

  def toRDD(): RDD[RDFTriple] = triples.rdd

  def unionAll(graphs: Seq[RDFGraphDataset]): RDFGraphDataset = {
    // the Dataframe based solution
//        return graphs.reduce(_ union _)

    // to limit the lineage, we convert to RDDs first, and use the SparkContext Union method for a sequence of RDDs
    val df: Option[DataFrame] = graphs match {
      case g :: Nil => Some(g.toDataFrame())
      case g :: _ => Some(g.toDataFrame().sqlContext.createDataFrame(
        g.toDataFrame().sqlContext.sparkContext.union(graphs.map(_.toDataFrame().rdd)),
        g.toDataFrame().schema
      ))
      case _ => None
    }

    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[RDFTriple]
    new RDFGraphDataset(df.get.as[RDFTriple])
  }
}
