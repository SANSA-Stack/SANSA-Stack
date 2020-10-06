package net.sansa_stack.owl.spark.writers

import org.eclipse.rdf4j.rio.helpers.AbstractRDFWriter
import org.semanticweb.owlapi.model.{OWLDocumentFormat, OWLOntology}
import org.semanticweb.owlapi.rio.RioRenderer

class SANSARioRenderer(
                        ont: OWLOntology,
                        rdfJsonWriter: AbstractRDFWriter,
                        docFormat: OWLDocumentFormat)
    extends RioRenderer(ont, rdfJsonWriter, docFormat) {

  override def renderOntologyHeader(): Unit = None
}
