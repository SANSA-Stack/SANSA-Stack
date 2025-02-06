package net.sansa_stack.query.spark.rdd.op

import org.apache.jena.query.{Dataset, DatasetFactory, Query, QueryExecutionFactory}
import org.apache.jena.rdf.model.Model
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

/**
 * Operations on the RDD[(String, Model)] type.
 *
 * The naming "RddOfNamedModelOps" allows for future introduction of "RddOfNamedGraphOps" in case there is demand
 * for these operations on Jena's Graph level.
 */
object RddOfNamedModelsOps {

  /**
   * Map each (name, model) pair to a dataset with the same information
   *
   * @param rdd
   * @return
   */
  def mapToDatasets(rdd: RDD[(String, Model)]): RDD[Dataset] = rdd.map({ case (graphName, model) =>
    val r = DatasetFactory.create
    r.addNamedModel(graphName, model)
    r
  })

  /**
   * Group and/or sort named models by their graph iri
   *
   * @param rdd
   * @param distinct        If false then models with the same key remain separated otherwise they become merged
   * @param sortGraphsByIri Whether to apply sorting in addition to grouping
   * @param numPartitions   Number of partitions to use for sorting; only applicable if sortGraphsByIri is true.
   * @return
   */
  def groupNamedModelsByGraphIri(
                                  rdd: RDD[(String, Model)],
                                  distinct: Boolean = true,
                                  sortGraphsByIri: Boolean = false,
                                  numPartitions: Int = 0): RDD[(String, Model)] = {
    var resultRdd: RDD[(String, Model)] = rdd

    if (distinct) {
      resultRdd = resultRdd.reduceByKey((g1, g2) => {
        g1.add(g2);
        g1
      })
    }

    if (numPartitions > 0) {
      if (sortGraphsByIri) {
        resultRdd = resultRdd.repartitionAndSortWithinPartitions(new HashPartitioner(numPartitions))
      } else {
        resultRdd = resultRdd.repartition(numPartitions)
      }
    }

    if (sortGraphsByIri) {
      resultRdd = resultRdd.sortByKey()
    }

    resultRdd
  }


  /**
   * Map each model using a SPARQL CONSTRUCT query.
   * They name remains unaffected
   *
   * @param rdd
   * @param query
   * @return
   */
  def mapWithSparql(rdd: RDD[(String, Model)], query: Query): RDD[(String, Model)] = {
    val queryBc = rdd.context.broadcast(query)

    rdd.map { case (key, model) =>

      val query = queryBc.value
      val qe = QueryExecutionFactory.create(query, model)
      var r: (String, Model) = null
      try {
        val tmp = qe.execConstruct

        // Split the datasets
        r = (key, tmp)
      } finally {
        qe.close()
      }

      r
    }
  }

  /**
   * Run a SPARQL query using execConstructDataset oon each model
   *
   * If the graph name needs to be part of the mapping operation then use
   * rddOfNamedModels.mapToDatasets.flotMapWithSparql
   */
  def flatMapWithSparql(rdd: RDD[(String, Model)], query: Query): RDD[(String, Model)] = {
    import scala.jdk.CollectionConverters._

    // def flatMapQuery(query: Query): RDD[Dataset] =
    val queryBc = rdd.context.broadcast(query)

    rdd.flatMap(in => {
      val model = in._2
      val query = queryBc.value
      val qe = QueryExecutionFactory.create(query, model)
      var r: Seq[(String, Model)] = null
      try {
        val tmp = qe.execConstructDataset

        // Split the datasets
        r = tmp.listNames.asScala.toSeq
          .map(name => (name, tmp.getNamedModel(name)))
          .toList // Materialize as a list because of subsequent close
      } finally {
        qe.close()
      }

      r
    })
  }

}
