package net.sansa_stack.query.spark.rdd.op

import org.aksw.jenax.arq.util.quad.DatasetGraphUtils
import org.apache.jena.graph.Triple
import org.apache.jena.query.{Dataset, DatasetFactory, Query, QueryExecutionFactory}
import org.apache.jena.rdf.model.{Model, Resource}
import org.apache.jena.sparql.core.Quad
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.exec.RowSet
import org.apache.spark.rdd.RDD

import java.util.Objects
import scala.jdk.CollectionConverters.asScalaIteratorConverter

/**
 * Operations for RDD[Dataset]
 */
object RddOfDatasetsOps {

  def naturalResources(rddOfDatasets: RDD[_ <: Dataset]): RDD[Resource] = {
    rddOfDatasets.flatMap(JenaDatasetOps.naturalResources)
  }

  /**
   * Flat map each dataset to its set of quads using its .find() method.
   * Includes triples in the default graph.
   */
  def flatMapToQuads(rddOfDatasets: RDD[_ <: Dataset]): RDD[Quad] = {
    rddOfDatasets.flatMap(_.asDatasetGraph().find.asScala)
  }

  /**
   * Flat map all quads in the dataset - in both named graphs and the default graph -
   * to triples
   */
  def flatMapToTriples(rddOfDatasets: RDD[_ <: Dataset]): RDD[Triple] = {
    flatMapToQuads(rddOfDatasets).map(_.asTriple)
  }

  def toNamedModels(rdd: RDD[_ <: Dataset]): RDD[(String, Model)] = {
    // TODO Add a flag to include the default graph under a certain name such as Quad.defaultGraph
    rdd
      .flatMap(ds => ds.listNames.asScala.map(iri => (iri, ds.getNamedModel(iri))))
  }

  /**
   * Group all graphs by their <b>named graph</b> IRIs.
   * Effectively merges triples from all named graphs with the same IRI.
   * Removes duplicated triples.
   *
   * Ignores default graphs which get lost.
   */
  def groupNamedGraphsByGraphIri(
                                  rdd: RDD[_ <: Dataset],
                                  distinct: Boolean = true,
                                  sortGraphsByIri: Boolean = false,
                                  numPartitions: Int = 0): RDD[Dataset] = {
    // Note: Model is usually ModelCom so we get out-of-the-box serialization
    // If we used Graph we'd have to deal with a lot more variation
    val step1 = toNamedModels(rdd)
    val step2 = RddOfNamedModelsOps.groupNamedModelsByGraphIri(step1, distinct, sortGraphsByIri, numPartitions)
    val result = RddOfNamedModelsOps.mapToDatasets(step2)

    result
  }

  /**
   * Deprecated due to scalability issues w.r.t. RAM when loading whole partitions into RAM in parallel with lotss of cores;
   * use flatMapWithSparql
   *
   * Run <b>a single</b> query over <b>each whole partition</b> (i.e. the query affects all graphs in the partition)
   */
  @deprecated
  def mapPartitionsWithSparql(rdd: RDD[_ <: Dataset], query: Query): RDD[Binding] = {
    // def flatMapQuery(query: Query): RDD[Dataset] =
    val queryBc = rdd.context.broadcast(query)

    Objects.requireNonNull(query)

    rdd.mapPartitions(datasets => {
      val all = DatasetFactory.create
      datasets.foreach(ds => ds.asDatasetGraph.find
        .forEachRemaining(x => all.asDatasetGraph().add(x)))

      val query = queryBc.value
      // TODO I don't get why the Query object is not serializablbe even though
      // the registrator for it is loaded ... investigae...

      val qe = QueryExecutionFactory.create(query, all)
      var r: Seq[Binding] = null
      try {
        r = RowSet.adapt(qe.execSelect).asScala.toList
      } finally {
        qe.close()
      }

      r.iterator
    })
  }

  /**
   * Run a select query on each individual dataset in the RDD and flat map it to the set of bindings
   */
  def flatMapWithSparqlSelect(rdd: RDD[_ <: Dataset], query: Query): RDD[Binding] = {
    Objects.requireNonNull(query)

    val queryBc = rdd.context.broadcast(query)
    rdd.flatMap(in => {
      val query = queryBc.value

      val qe = QueryExecutionFactory.create(query, in)
      var r: Seq[Binding] = null
      try {
        r = RowSet.adapt(qe.execSelect).asScala.toList
      } finally {
        qe.close()
      }

      r
    })
  }


  /**
   * Run a select query on each partition (merges all datasets in a partition and thus may remove duplicates) dataset in the RDD
   */
  def selectWithSparqlPerPartition(rdd: RDD[_ <: Dataset], query: Query): RDD[Binding] = {
    val queryBc = rdd.context.broadcast(query)

    Objects.requireNonNull(query)

    rdd.mapPartitions(it => {
      val dataset = DatasetFactory.create()
      it.foreach(item => DatasetGraphUtils.addAll(dataset.asDatasetGraph(), item.asDatasetGraph()))

      val query = queryBc.value

      val qe = QueryExecutionFactory.create(query, dataset)
      var r: Seq[Binding] = null
      try {
        r = RowSet.adapt(qe.execSelect).asScala.toList
      } finally {
        qe.close()
      }

      r.iterator
    })
  }


  def flatMapWithSparql(rddOfDatasets: RDD[_ <: Dataset], query: Query): RDD[Dataset] = {
    // def flatMapQuery(query: Query): RDD[Dataset] =
    val queryBc = rddOfDatasets.context.broadcast(query)

    rddOfDatasets.flatMap(in => {
      // TODO I don't get why the Query object is not serializablbe even though
      // the registrator for it is loaded ... investigae...
      val query = queryBc.value // QueryFactory.create(queryStr, Syntax.syntaxARQ);

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
          .toList // Materialize as a list because of subsequent close
      } finally {
        qe.close()
      }

      r
    })
  }

  def filterWithSparql(rddOfDatasets: RDD[_ <: Dataset], query: Query, drop: Boolean): RDD[_ <: Dataset] = {
    // def flatMapQuery(query: Query): RDD[Dataset] =
    val queryBc = rddOfDatasets.context.broadcast(query)
    rddOfDatasets.filter(in => {
      // TODO Make deserialization of query work
      val rawQuery = queryBc.value
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
