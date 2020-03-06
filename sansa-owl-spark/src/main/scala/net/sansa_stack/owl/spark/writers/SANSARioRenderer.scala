package net.sansa_stack.owl.spark.writers

import org.eclipse.rdf4j.rio.helpers.AbstractRDFWriter
import org.semanticweb.owlapi.formats.{RDFJsonLDDocumentFormat, RioRDFNonPrefixDocumentFormat}
import org.semanticweb.owlapi.model.OWLOntology
import org.semanticweb.owlapi.rio.RioRenderer

class SANSARioRenderer(ont: OWLOntology, rdfJsonWriter: AbstractRDFWriter, docFormat: RioRDFNonPrefixDocumentFormat)
  extends RioRenderer(ont, rdfJsonWriter, docFormat) {

  override def renderOntologyHeader(): Unit = None
}
