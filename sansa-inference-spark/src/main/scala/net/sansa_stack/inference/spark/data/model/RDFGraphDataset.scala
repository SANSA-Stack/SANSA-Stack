package net.sansa_stack.inference.spark.data.model

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import net.sansa_stack.inference.data.{RDFTriple, SQLSchema, SQLSchemaDefault}
import net.sansa_stack.inference.spark.data.model.TripleUtils._
import org.apache.jena.graph.{Node, Triple}

/**
  * A data structure that comprises a set of triples.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFGraphDataset(override val triples: Dataset[Triple])
    extends AbstractRDFGraphSpark[Dataset, Node, Triple, RDFGraphDataset](triples) {

  override def find(s: Option[Node] = None, p: Option[Node] = None, o: Option[Node] = None): RDFGraphDataset = {
    new RDFGraphDataset(
      triples.filter(
        t =>
          (s.isEmpty || s.get.isVariable || t.s == s.get) &&
            (p.isEmpty || p.get.isVariable || t.p == p.get) &&
            (o.isEmpty || o.get.isVariable || t.o == o.get)
      )
    )
  }

  override def find(triple: Triple): RDFGraphDataset =
    find(Some(triple.getSubject), Some(triple.getPredicate), Some(triple.getObject))

  def union(graph: RDFGraphDataset): RDFGraphDataset = {
    new RDFGraphDataset(triples.union(graph.triples))
  }

  def unionAll(graphs: Seq[RDFGraphDataset]): RDFGraphDataset = {
    // the Dataframe based solution
    return graphs.reduce(_ union _)

//    // to limit the lineage, we convert to RDDs first, and use the SparkContext Union method for a sequence of RDDs
//    val df: Option[DataFrame] = graphs match {
//      case g :: Nil => Some(g.toDataFrame())
//      case g :: _ => Some(g.toDataFrame().sqlContext.createDataFrame(
//        g.toDataFrame().sqlContext.sparkContext.union(graphs.map(_.toDataFrame().rdd)),
//        g.toDataFrame().schema
//      ))
//      case _ => None
//    }
//
//    val spark = graphs(0).triples.sparkSession.sqlContext
//    import spark.implicits._
//    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[RDFTriple]
//    new RDFGraphDataset(df.get.as[RDFTriple])
  }

  override def intersection(graph: RDFGraphDataset): RDFGraphDataset =
    new RDFGraphDataset(triples.intersect(graph.triples))

  override def difference(graph: RDFGraphDataset): RDFGraphDataset = new RDFGraphDataset(triples.except(graph.triples))

  def distinct(): RDFGraphDataset = {
    new RDFGraphDataset(triples.distinct())
  }

  def size(): Long = {
    triples.count()
  }

  def cache(): this.type = {
    triples.cache()
    this
  }

  def toDataFrame(sparkSession: SparkSession, schema: SQLSchema = SQLSchemaDefault): DataFrame = triples.toDF()

  def toRDD(): RDD[Triple] = triples.rdd

}
