package net.sansa_stack.rdf.spark.model.rdd

import java.util.Objects

import org.aksw.jena_sparql_api.utils.IteratorResultSetBinding
import org.apache.jena.graph.{Graph, GraphUtil}
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory, Resource}
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.util.DatasetUtils
import org.apache.jena.sparql.util.compose.DatasetLib
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

/**
 * Operations for RDD[Dataset]
 */
object RddOfDatasetOps {

  @inline def naturalResources(rddOfDatasets: RDD[_ <: Dataset]): RDD[Resource] = {
    rddOfDatasets.flatMap(JenaDatasetOps.naturalResources)
  }

  /**
   * Group all graphs by their <b>named graph</b> IRIs.
   * Effectively merges triples from all named graphs with the same IRI.
   * Removes duplicated triples.
   *
   * Ignores default graphs which get lost.
   */
  def groupNamedGraphsByGraphIri(rdd: RDD[_ <: Dataset]): RDD[Dataset] = {
    import collection.JavaConverters._
    // Note: Model is usually ModelCom so we get out-of-the-box serialization
    // If we used Graph we'd have to deal with a lot more variation
    val graphNameAndModel: RDD[(String, Model)] = rdd
      .flatMap(ds => ds.listNames.asScala.map(iri => (iri, ds.getNamedModel(iri))))

    val result: RDD[Dataset] = graphNameAndModel
      .reduceByKey((g1, g2) => { g1.add(g2); g1 })
      .sortByKey()
      .map({ case (graphName, model) =>
        val r = DatasetFactory.create
        r.addNamedModel(graphName, model)
        r
      })

    result
  }

  /* run <b>a single</b> query over <b>each whole partition</b> (i.e. the query affects all graphs in the partition) */
  def mapPartitionsWithSparql(rdd: RDD[_ <: Dataset], query: Query): RDD[Binding] = {
    // def flatMapQuery(query: Query): RDD[Dataset] =
    val queryBc = rdd.context.broadcast(query)

    Objects.requireNonNull(query)

    rdd.mapPartitions(datasets =>
    {
      val all = DatasetFactory.create
      datasets.foreach(ds => ds.asDatasetGraph.find
        .forEachRemaining(x => all.asDatasetGraph().add(x)))

      val query = queryBc.value
      // TODO I don't get why the Query object is not serializablbe even though
      // the registrator for it is loaded ... investigae...

      val qe = QueryExecutionFactory.create(query, all)
      var r: Seq[Binding] = null
      try {
        r = new IteratorResultSetBinding(qe.execSelect).asScala.toList
      } finally {
        qe.close()
      }

      r.iterator
    })
  }

  /**
   * Run a select query on each individual dataset in the RDD
   */
  def selectWithSparql(rdd: RDD[_ <: Dataset], query: Query): RDD[Binding] = {
    val queryBc = rdd.context.broadcast(query)

    Objects.requireNonNull(query)

    rdd.flatMap(in => {
      val query = queryBc.value

      val qe = QueryExecutionFactory.create(query, in)
      var r: Seq[Binding] = null
      try {
        r = new IteratorResultSetBinding(qe.execSelect).asScala.toList
      } finally {
        qe.close()
      }

      r
    })
  }

  @inline def flatMapWithSparql(rddOfDatasets: RDD[_ <: Dataset], queryStr: String): RDD[Dataset] = {
    // def flatMapQuery(query: Query): RDD[Dataset] =
    rddOfDatasets.flatMap(in => {
      // TODO I don't get why the Query object is not serializablbe even though
      // the registrator for it is loaded ... investigae...
      val query = QueryFactory.create(queryStr, Syntax.syntaxARQ);

      val qe = QueryExecutionFactory.create(query, in)
      var r: Seq[Dataset] = null
      try {
        val tmp = qe.execConstructDataset

        // Split the datasets
        r = tmp.listNames.asScala.toSeq
          .map(name => {
            val model = tmp.getNamedModel(name)
            val ds = DatasetFactory.create
            ds.addNamedModel(name, model)
            ds
          })
      } finally {
        qe.close()
      }

      r
    })
  }

  @inline def filterWithSparql(rddOfDatasets: RDD[_ <: Dataset], queryStr: String, drop: Boolean): RDD[_ <: Dataset] = {
    // def flatMapQuery(query: Query): RDD[Dataset] =
    rddOfDatasets.filter(in => {
      // TODO Make deserialization of query work
      val rawQuery = QueryFactory.create(queryStr, Syntax.syntaxARQ)
      val query = RddOfRdfOpUtils.enforceQueryAskType(rawQuery)

      val qe = QueryExecutionFactory.create(query, in)
      var r = false
      try {
        r = qe.execAsk()
        // Invert the result if drop is true
        r = if (drop) !r else r

      } finally {
        qe.close()
      }

      r
    })
  }

  /*
 @inline def sparqlSortBy(rddOfDatasets: RDD[_ <: Dataset], queryStr: String, descending: Boolean): RDD[_ <: Dataset] = {
   // def flatMapQuery(query: Query): RDD[Dataset] =
   rddOfDatasets.filter(in => {
     // TODO Make deserialization of query work
     val rawQuery = QueryFactory.create(queryStr, Syntax.syntaxARQ)
     val query = RddOfRdfOpUtils.enforceQueryAskType(rawQuery)

     val qe = QueryExecutionFactory.create(query, in)
     var r = false
     try {
       r = qe.execAsk()
       // Invert the result if drop is true
       r = if (drop) !r else r

     } finally {
       qe.close()
     }

     r
   })
 }
*/

}
