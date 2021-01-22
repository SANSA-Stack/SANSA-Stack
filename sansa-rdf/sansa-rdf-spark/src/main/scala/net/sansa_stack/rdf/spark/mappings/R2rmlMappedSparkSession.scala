package net.sansa_stack.rdf.spark.mappings

import org.apache.jena.rdf.model.Model
import org.apache.spark.sql.SparkSession

/**
 * Class intended to exchage r2rml mappings between components,
 * such as between a partitioner and a SPARQL query engine factory.
 *
 * All information relevant to access a sparkSession
 * as a virtual knowledge graph based on r2rml
 * should go into this class.
 *
 * An explicit goal of this class is for it to hold sufficient information
 * to enable configuration of mapping engines such as sparqlify or ontop.
 *
 * @param sparkSession
 * @param r2rmlModel
 */
case class R2rmlMappedSparkSession(sparkSession: SparkSession, r2rmlModel: Model) {
}
