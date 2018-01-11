package net.sansa_stack.rdf.spark.qualityassessment.vocabularies

/*
 * Data Quality Vocabulary
 * Vocabulary Definitions from https://www.w3.org/TR/vocab-dqv/
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

  //Indicates the metric being observed.
  val isMeasurementOf = "dqv:isMeasurementOf"

  //Indicates the data set of which this observation is a part.
  val dataSet = "<http://purl.org/linked-data/cube#dataSet>"

  //Refers to the resource (e.g., a dataset, a linkset, a graph, a set of triples) on which the quality measurement is performed.
  val computedOn = "dqv:computedOn"

}