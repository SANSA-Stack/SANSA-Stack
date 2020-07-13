package net.sansa_stack.rdf.spark

import org.apache.jena.graph.Graph
import org.apache.jena.query._
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.syntax.ElementSubQuery
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

package object ops {

  object RddOfRdfOpUtils {
    def enforceQueryAskType(query: Query): Query = {
      val result = if (query.isAskType) {
        query
      } else {
        val inner = query.cloneQuery()
        val outer = new Query()

        // Copy prefixes to the outer query and clear them on the inner one
        outer.setPrefixMapping(inner.getPrefixMapping)
        inner.getPrefixMapping.clearNsPrefixMap()

        outer.setQueryAskType()
        outer.setQueryPattern(new ElementSubQuery(inner))
        outer
      }

      result
    }
  }


  /**
   * Operations for RDD[Dataset]
   */
  object RddOfDatasetsOps {

    @inline def sparqlFlatMap(rddOfDatasets: RDD[_ <: Dataset], queryStr: String): RDD[Dataset] = {
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

    @inline def sparqlFilter(rddOfDatasets: RDD[_ <: Dataset], queryStr: String, drop: Boolean): RDD[_ <: Dataset] = {
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

    // implicit class DatasetOps[T <: Dataset](dataset: RDD[T]) {
    implicit class RdfOfDatasetOpsImpl(dataset: RDD[Dataset]) {

      /**
       * Execute an <b>extended</b> CONSTRUCT SPARQL query on an RDD of Datasets and
       * yield every constructed named graph or default graph as a separate item
       * Extended means that the use of GRAPH is allowed in the template,
       * such as in CONSTRUCT { GRAPH ?g { ... } } WHERE { }
       *
       * @param query
       * @return
       */
      @inline def sparqlFlatMap(query: Query): RDD[Dataset] = RddOfDatasetsOps.sparqlFlatMap(dataset, query.toString())

      @inline def sparqlFilterKeep(query: Query): RDD[_ <: Dataset] = sparqlFilter(query, false)
      @inline def sparqlFilterDrop(query: Query): RDD[_ <: Dataset] = sparqlFilter(query, true)
      @inline def sparqlFilter(query: Query, drop: Boolean = false): RDD[_ <: Dataset] = RddOfDatasetsOps.sparqlFilter(dataset, query.toString(), drop)

      @inline def sparqlSortBy(query: Query, descending: Boolean = false): RDD[_ <: Dataset] = RddOfDatasetsOps.sparqlFilter(dataset, query.toString(), drop)
    }

  }


  /**
   * Operations for RDD[Model]
   */
  object RddOfModelsOps {

    @inline def sparqlFlatMap(rddOfModels: RDD[_ <: Model], queryStr: String, emitEmptyModels: Boolean = false): RDD[Model] = {
      // def flatMapQuery(query: Query): RDD[Dataset] =
      rddOfModels.flatMap(in => {
        // TODO I don't get why the Query object is not serializablbe even though
        // the registrator for it is loaded ... investigae...
        val query = QueryFactory.create(queryStr, Syntax.syntaxARQ);

        val qe = QueryExecutionFactory.create(query, in)
        var r: Seq[Model] = null
        try {
          val tmp = qe.execConstruct()

          r = if (!emitEmptyModels && tmp.isEmpty) Seq() else Seq(tmp)

        } finally {
          qe.close()
        }

        r
      })
    }

    @inline def sparqlFilter(rddOfModels: RDD[_ <: Model], queryStr: String, drop: Boolean): RDD[_ <: Model] = {
      // def flatMapQuery(query: Query): RDD[Dataset] =
      rddOfModels.filter(in => {
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

    // implicit class DatasetOps[T <: Dataset](dataset: RDD[T]) {
    implicit class RdfOfModelsOpsImpl(rddOfModels: RDD[Dataset]) {

      /**
       * Execute an <b>extended</b> CONSTRUCT SPARQL query on an RDD of Datasets and
       * yield every constructed named graph or default graph as a separate item
       * Extended means that the use of GRAPH is allowed in the template,
       * such as in CONSTRUCT { GRAPH ?g { ... } } WHERE { }
       *
       * @param query
       * @return
       */
      @inline def sparqlFlatMap(query: Query): RDD[Model] = RddOfModelsOps.sparqlFlatMap(rddOfModels, query.toString())

      @inline def sparqlFilterKeep(query: Query): RDD[_ <: Model] = sparqlFilter(query, false)
      @inline def sparqlFilterDrop(query: Query): RDD[_ <: Model] = sparqlFilter(query, true)
      @inline def sparqlFilter(query: Query, drop: Boolean = false): RDD[_ <: Model] = RddOfModelsOps.sparqlFilter(rddOfModels, query.toString(), drop)

    }

  }
}
