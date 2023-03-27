package net.sansa_stack.query.spark.rdd.op

import org.apache.jena.query.{QueryExecutionFactory, QueryFactory, Syntax}
import org.apache.jena.rdf.model.Model
import org.apache.spark.rdd.RDD

/**
 * Operations for RDD[Model]
 *
 * Requires serializers for [[Model]] implementations to be registered with kryo
 *
 */
object RddOfModelsOps {

  @inline def sparqlMap(rddOfModels: RDD[_ <: Model], queryStr: String): RDD[Model] = {
    // def flatMapQuery(query: Query): RDD[Dataset] =
    rddOfModels.map(in => {
      // TODO Make spark-native deserialization of Query work
      // TODO I don't get why the Query object is not serializable even though
      // the registrator for it is loaded ... investigate...
      val query = QueryFactory.create(queryStr, Syntax.syntaxARQ);

      val qe = QueryExecutionFactory.create(query, in)
      var r: Model = null
      try {
        r = qe.execConstruct()
      } finally {
        qe.close()
      }

      r
    })
  }

  @inline def sparqlFilter(rddOfModels: RDD[_ <: Model], queryStr: String, drop: Boolean): RDD[_ <: Model] = {
    // def flatMapQuery(query: Query): RDD[Dataset] =
    rddOfModels.filter(in => {
      // TODO Make spark-native deserialization of Query work
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

}
