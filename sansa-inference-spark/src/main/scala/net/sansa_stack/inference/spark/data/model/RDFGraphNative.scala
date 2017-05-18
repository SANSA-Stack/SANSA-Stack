package net.sansa_stack.inference.spark.data.model

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import net.sansa_stack.inference.data.{SQLSchema, SQLSchemaDefault}
import net.sansa_stack.inference.spark.data.model.TripleUtils._

/**
  * A data structure that comprises a set of triples based on an RDD.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFGraphNative(override val triples: RDD[Triple])
    extends AbstractRDFGraphSpark[RDD, Node, Triple, RDFGraphNative](triples) {

  override def find(s: Option[Node] = None, p: Option[Node] = None, o: Option[Node] = None): RDFGraphNative = {
    new RDFGraphNative(
      triples.filter(
        t =>
            (s.isEmpty || s.get.isVariable || t.s == s.get) &&
            (p.isEmpty || p.get.isVariable || t.p == p.get) &&
            (o.isEmpty || o.get.isVariable || t.o == o.get)
      )
    )
  }

  override def find(triple: Triple): RDFGraphNative =
    find(Some(triple.getSubject), Some(triple.getPredicate), Some(triple.getObject))

  override def union(graph: RDFGraphNative): RDFGraphNative = {
    new RDFGraphNative(triples.union(graph.toRDD()))
  }

  override def unionAll(graphs: Seq[RDFGraphNative]): RDFGraphNative = {
    //    return graphs.reduceLeft(_ union _)
    val first = graphs.head
    new RDFGraphNative(first.triples.sparkContext.union(graphs.map(g => g.toRDD())))
  }

  /**
    * Returns a new RDF graph that contains the intersection of the current RDF graph with the given RDF graph.
    *
    * @param graph the other RDF graph
    * @return the intersection of both RDF graphs
    */
  override def intersection(graph: RDFGraphNative): RDFGraphNative =
    new RDFGraphNative(this.triples.intersection(graph.triples))

  /**
    * Returns a new RDF graph that contains the difference between the current RDF graph and the given RDF graph.
    *
    * @param graph the other RDF graph
    * @return the difference of both RDF graphs
    */
  override def difference(graph: RDFGraphNative): RDFGraphNative =
    new RDFGraphNative(this.triples.subtract(graph.triples))

  override def distinct(): RDFGraphNative = {
    new RDFGraphNative(triples.distinct())
  }

  override def size(): Long = {
    triples.count()
  }

  override def cache(): this.type = {
    triples.cache()
    this
  }

  override def toRDD(): RDD[Triple] = triples

  override def toDataFrame(sparkSession: SparkSession, schema: SQLSchema = SQLSchemaDefault): DataFrame = {
    // convert RDD to DataFrame

    // generate the schema based on the string of schema
    val schemaStructure = StructType(
      Seq(
        StructField(schema.subjectCol, StringType, nullable = false),
        StructField(schema.predicateCol, StringType, nullable = false),
        StructField(schema.objectCol, StringType, nullable = false)
      )
    )

    // convert triples RDD to rows
    val rowRDD = triples.map(t => Row(t.s, t.p, t.o))

    // apply the schema to the RDD
    val triplesDataFrame = sparkSession.createDataFrame(rowRDD, schemaStructure)

    // register the DataFrame as a table
    triplesDataFrame.createOrReplaceTempView(schema.triplesTable)

    triplesDataFrame
  }
}
