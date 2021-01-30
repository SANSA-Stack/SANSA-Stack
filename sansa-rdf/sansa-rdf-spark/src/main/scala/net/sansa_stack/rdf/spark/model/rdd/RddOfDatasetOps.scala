package net.sansa_stack.rdf.spark.model.rdd

import java.util.Objects

import org.aksw.jena_sparql_api.utils.IteratorResultSetBinding
import org.apache.jena.query._
import org.apache.jena.rdf.model.Resource
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


  /* run one query per partition (i.e. over multiple graphs) */
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
    // def flatMapQuery(query: Query): RDD[Dataset] =
    val queryBc = rdd.context.broadcast(query)

    Objects.requireNonNull(query)

    rdd.flatMap(in => {
      val query = queryBc.value
      // TODO I don't get why the Query object is not serializablbe even though
      // the registrator for it is loaded ... investigae...

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
