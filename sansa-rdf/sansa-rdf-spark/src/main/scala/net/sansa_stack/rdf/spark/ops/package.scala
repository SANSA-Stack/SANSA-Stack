package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.model.rdd.{RddOfDatasetOps, RddOfModelsOps, RddOfResourcesOps, RddOfTriplesOps}
import org.apache.jena.graph.Triple
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, RDFNode, Resource}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object ops {
  implicit class RddOfTriplesOpsImpl(rddOfTriples: RDD[Triple]) {

    @inline def filterPredicates(predicateIris: Set[String]): RDD[Triple] = RddOfTriplesOps.filterPredicates(rddOfTriples, predicateIris)
  }


  // implicit class DatasetOps[T <: Dataset](dataset: RDD[T]) {
  implicit class RddOfModelsOpsImpl(rddOfModels: RDD[Model]) {

    /**
     * Execute an <b>extended</b> CONSTRUCT SPARQL query on an RDD of Datasets and
     * yield every constructed named graph or default graph as a separate item
     * Extended means that the use of GRAPH is allowed in the template,
     * such as in CONSTRUCT { GRAPH ?g { ... } } WHERE { }
     *
     * @param query
     * @return
     */
    @inline def sparqlMap(query: Query): RDD[Model] = RddOfModelsOps.sparqlMap(rddOfModels, query.toString())

    @inline def sparqlFilterKeep(query: Query): RDD[_ <: Model] = sparqlFilter(query, false)
    @inline def sparqlFilterDrop(query: Query): RDD[_ <: Model] = sparqlFilter(query, true)
    @inline def sparqlFilter(query: Query, drop: Boolean = false): RDD[_ <: Model] = RddOfModelsOps.sparqlFilter(rddOfModels, query.toString(), drop)
  }

  implicit class RddOfResourcesOpsImpl(rddOfResources: RDD[_ <: Resource]) {
    @inline def mapAs[T <: RDFNode](clazz: Class[T]): RDD[T] = RddOfResourcesOps.mapAs(ClassTag(clazz), rddOfResources, clazz)
    @inline def models(): RDD[Model] = RddOfResourcesOps.mapToModels(rddOfResources)
  }

  implicit class RddOfDatasetsOpsImpl(rddOfDatasets: RDD[Dataset]) {
    /**
     * Execute an <b>extended</b> CONSTRUCT SPARQL query on an RDD of Datasets and
     * yield every constructed named graph or default graph as a separate item
     * Extended means that the use of GRAPH is allowed in the template,
     * such as in CONSTRUCT { GRAPH ?g { ... } } WHERE { }
     *
     * @param query
     * @return
     */
    @inline def sparqlFlatMap(query: Query): RDD[Dataset] = RddOfDatasetOps.flatMapWithSparql(rddOfDatasets, query.toString())

    @inline def sparqlFilterKeep(query: Query): RDD[_ <: Dataset] = sparqlFilter(query, false)
    @inline def sparqlFilterDrop(query: Query): RDD[_ <: Dataset] = sparqlFilter(query, true)
    @inline def sparqlFilter(query: Query, drop: Boolean = false): RDD[_ <: Dataset] = RddOfDatasetOps.filterWithSparql(rddOfDatasets, query.toString(), drop)

    @inline def mapToNaturalResources(): RDD[Resource] = RddOfDatasetOps.naturalResources(rddOfDatasets)
  }

}
