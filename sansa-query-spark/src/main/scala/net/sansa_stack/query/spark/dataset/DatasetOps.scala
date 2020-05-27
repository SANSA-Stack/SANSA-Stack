package net.sansa_stack.query.spark.dataset

import org.apache.jena.query.{Dataset, DatasetFactory, Query, QueryExecutionFactory, QueryFactory, Syntax}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._


object DatasetOps {

  @inline def flatMapQueryCore(dataset: RDD[_ <: Dataset], queryStr: String): RDD[Dataset] = {
    // def flatMapQuery(query: Query): RDD[Dataset] =
    dataset.flatMap(in => {
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

   // implicit class DatasetOps[T <: Dataset](dataset: RDD[T]) {
   implicit class DatasetOpsImpl(dataset: RDD[Dataset]) {
     @inline def sparqlFlatMap(query: Query): RDD[Dataset] = flatMapQueryCore(dataset, query.toString())
  }
}
