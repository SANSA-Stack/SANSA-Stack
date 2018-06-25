package net.sansa_stack.rdf.spark.utils

/**
 * @author Gezim Sejdiu
 */
object StatsPrefixes {
  val RDF_CLASS = "http://www.w3.org/2000/01/rdf-schema#Class"
  val OWL_CLASS = "http://www.w3.org/2002/07/owl#Class"
  val RDFS_CLASS = "http://www.w3.org/2000/01/rdf-schema#Class"
  val RDFS_SUBCLASS_OF = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
  val RDFS_SUBPROPERTY_OF = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
  val RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
  val RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  val XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
  val XSD_INT = "http://www.w3.org/2001/XMLSchema#int"
  val XSD_float = "http://www.w3.org/2001/XMLSchema#float"
  val XSD_datetime = "http://www.w3.org/2001/XMLSchema#datetime"
  val OWL_SAME_AS = "http://www.w3.org/2002/07/owl#sameAs"
  val OWL_OBJECT_PROPERTY = "http://www.w3.org/2002/07/owl#ObjectProperty"
  val RDF_PROPERTY = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"
}

object Vocabularies {

  /**
   * Vocabulary definitions from https://www.w3.org/TR/vocab-dcat/
   */
  object DCAT {

    /** The namespace of the DAQ (https://www.w3.org/TR/2015/WD-vocab-dqv-20150625/ ) vocabulary */
    val DAQ_NS = "http://purl.org/eis/vocab/daq#"

    val computedBy = DAQ_NS + "#computedBy"
  }

}
