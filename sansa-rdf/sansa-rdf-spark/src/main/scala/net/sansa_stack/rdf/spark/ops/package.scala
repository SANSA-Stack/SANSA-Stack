package net.sansa_stack.rdf.spark

import net.sansa_stack.query.spark.rdd.op.{RddOfDatasetsOps, RddOfModelsOps, RddOfResourcesOps}
import net.sansa_stack.rdf.spark.rdd.op.RddOfTriplesOps
import org.apache.jena.graph.Triple
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, RDFNode, Resource}
import org.apache.jena.sparql.core.Quad
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object ops {
  implicit class RddOfTriplesOpsImpl(rddOfTriples: RDD[Triple]) {

    def filterPredicates(predicateIris: Set[String]): RDD[Triple] = RddOfTriplesOps.filterPredicates(rddOfTriples, predicateIris)
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
    def sparqlMap(query: Query): RDD[Model] = RddOfModelsOps.sparqlMap(rddOfModels, query.toString())

    def sparqlFilterKeep(query: Query): RDD[_ <: Model] = sparqlFilter(query, false)
    def sparqlFilterDrop(query: Query): RDD[_ <: Model] = sparqlFilter(query, true)
    def sparqlFilter(query: Query, drop: Boolean = false): RDD[_ <: Model] = RddOfModelsOps.sparqlFilter(rddOfModels, query.toString(), drop)
  }

  implicit class RddOfResourcesOpsImpl(rddOfResources: RDD[_ <: Resource]) {
    def mapAs[T <: RDFNode](clazz: Class[T]): RDD[T] = RddOfResourcesOps.mapAs(ClassTag(clazz), rddOfResources, clazz)
    def models(): RDD[Model] = RddOfResourcesOps.mapToModels(rddOfResources)
  }

  implicit class RddOfDatasetsOpsImpl(rddOfDatasets: RDD[_ <: Dataset]) {
    /**
     * Execute an <b>extended</b> CONSTRUCT SPARQL query on an RDD of Datasets and
     * yield every constructed named graph or default graph as a separate item
     * Extended means that the use of GRAPH is allowed in the template,
     * such as in CONSTRUCT { GRAPH ?g { ... } } WHERE { }
     *
     * @param query
     * @return
     */
    def sparqlFlatMap(query: Query): RDD[Dataset] = RddOfDatasetsOps.flatMapWithSparql(rddOfDatasets, query)

    def sparqlFilterKeep(query: Query): RDD[_ <: Dataset] = sparqlFilter(query, false)
    def sparqlFilterDrop(query: Query): RDD[_ <: Dataset] = sparqlFilter(query, true)
    def sparqlFilter(query: Query, drop: Boolean = false): RDD[_ <: Dataset] = RddOfDatasetsOps.filterWithSparql(rddOfDatasets, query, drop)

    def mapToNaturalResources(): RDD[Resource] = RddOfDatasetsOps.naturalResources(rddOfDatasets)

    def flatMapToTriples(): RDD[Triple] = RddOfDatasetsOps.flatMapToTriples(rddOfDatasets)
    def flatMapToQuads(): RDD[Quad] = RddOfDatasetsOps.flatMapToQuads(rddOfDatasets)
  }

}
