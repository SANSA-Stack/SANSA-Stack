package net.sansa_stack.rdf.spark.model.rdd

import org.apache.jena.query._
import org.apache.jena.rdf.model.Resource
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

/**
 * Operations for RDD[Dataset]
 */
object RddOfDatasetsOps {

  @inline def naturalResources(rddOfDatasets: RDD[_ <: Dataset]): RDD[Resource] = {
    rddOfDatasets.flatMap(JenaDatasetOps.naturalResources)
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
