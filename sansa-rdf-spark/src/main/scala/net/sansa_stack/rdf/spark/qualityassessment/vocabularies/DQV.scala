package net.sansa_stack.rdf.spark.qualityassessment.vocabularies

/**
 * Data Quality Vocabulary
 * Vocabulary Definitions from https://www.w3.org/TR/vocab-dqv/
 *
 * @author Gezim Sejdiu
 */
object DQV {

  val daq = "<http://purl.org/eis/vocab/daq#"
  val dcat = "<http://www.w3.org/ns/dcat#>"
  val dcterms = "<http://purl.org/dc/terms/>"
  val dqv = "<http://www.w3.org/ns/dqv#>"
  val duv = "<http://www.w3.org/ns/duv#>"
  val oa = "<http://www.w3.org/ns/oa#>"
  val prov = "<http://www.w3.org/ns/prov#>"
  val sdmx_attribute = "<http://purl.org/linked-data/sdmx/2009/attribute#>"
  val skos = "<http://www.w3.org/2004/02/skos/core#>"
  val RDFS = "<http://www.w3.org/2000/01/rdf-schema#>"

  val cclicence = "http://creativecommons.org/ns#license"
  val dbolicense = "http://dbpedia.org/ontology/>"
  val xhtmllicense = "http://www.w3.org/1999/xhtml/vocab#license"
  val dclicence = "http://purl.org/dc/elements/1.1/licence"
  val dcrights = "http://purl.org/dc/elements/1.1/rights"
  val dctlicense = "http://purl.org/dc/terms/license"
  val dbplicence = "http://dbpedia.org/property/licence"
  val doaplicense = "http://usefulinc.com/ns/doap#license"
  val dctrights = "http://www.w3.org/ns/dcat/rights"
  val schemalicense = "https://schema.org/license"

  // Indicates the metric being observed.
  val isMeasurementOf = "dqv:isMeasurementOf"

  // Indicates the data set of which this observation is a part.
  val dataSet = "http://purl.org/linked-data/cube#dataSet"
  val dqv_description = "http://www.w3.org/ns/dqv#description"
  // Refers to the resource (e.g., a dataset, a linkset, a graph, a set of triples) on which the quality measurement is performed.
  val computedOn = "dqv:computedOn"

}
