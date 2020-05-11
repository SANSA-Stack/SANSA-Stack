package net.sansa_stack.owl.spark.writers
import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter, PrintWriter}

import scala.collection.JavaConverters._

import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{OWLAnnotationAssertionAxiom, OWLAnnotationPropertyDomainAxiom, OWLAnnotationPropertyRangeAxiom, OWLAsymmetricObjectPropertyAxiom, OWLClassAssertionAxiom, OWLDataPropertyAssertionAxiom, OWLDataPropertyDomainAxiom, OWLDataPropertyRangeAxiom, OWLDatatypeDefinitionAxiom, OWLDeclarationAxiom, OWLDifferentIndividualsAxiom, OWLDisjointClassesAxiom, OWLDisjointDataPropertiesAxiom, OWLDisjointObjectPropertiesAxiom, OWLDisjointUnionAxiom, OWLEquivalentClassesAxiom, OWLEquivalentDataPropertiesAxiom, OWLEquivalentObjectPropertiesAxiom, OWLFunctionalDataPropertyAxiom, OWLFunctionalObjectPropertyAxiom, OWLHasKeyAxiom, OWLInverseFunctionalObjectPropertyAxiom, OWLInverseObjectPropertiesAxiom, OWLIrreflexiveObjectPropertyAxiom, OWLNegativeDataPropertyAssertionAxiom, OWLNegativeObjectPropertyAssertionAxiom, OWLObjectPropertyAssertionAxiom, OWLObjectPropertyDomainAxiom, OWLObjectPropertyRangeAxiom, OWLOntologyWriterConfiguration, OWLReflexiveObjectPropertyAxiom, OWLSameIndividualAxiom, OWLSubAnnotationPropertyOfAxiom, OWLSubClassOfAxiom, OWLSubDataPropertyOfAxiom, OWLSubObjectPropertyOfAxiom, OWLSubPropertyChainOfAxiom, OWLSymmetricObjectPropertyAxiom, OWLTransitiveObjectPropertyAxiom}
import org.semanticweb.owlapi.owlxml.renderer.{OWLXMLObjectRenderer, OWLXMLWriter}

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

object OWLXMLWriter extends OWLWriterBase {
  override def save(filePath: String, owlAxioms: OWLAxiomsRDD): Unit =
    owlAxioms.mapPartitions(partition => if (partition.hasNext) {
      partition.map(axiom => {
        val os = new ByteArrayOutputStream()
        val osWriter = new OutputStreamWriter(os)
        val buffPrintWriter = new PrintWriter(new BufferedWriter(osWriter))

        val man = OWLManager.createOWLOntologyManager()
        val config = new OWLOntologyWriterConfiguration
        config.withBannersEnabled(false)
        config.withIndenting(false)
        config.withLabelsAsBanner(false)
        config.withUseNamespaceEntities(false)
        man.setOntologyWriterConfiguration(config)

        assert(man.getOntologyWriterConfiguration == config)
        val ont = man.createOntology(Seq(axiom).asJava)
        assert(ont.getOWLOntologyManager.getOntologyWriterConfiguration == config)

        val renderer = new OWLXMLObjectRenderer(new OWLXMLWriter(buffPrintWriter, ont))

        axiom match {
          case a: OWLDatatypeDefinitionAxiom => renderer.visit(a)
          case a: OWLSubAnnotationPropertyOfAxiom => renderer.visit(a)
          case a: OWLAnnotationPropertyRangeAxiom => renderer.visit(a)
          case a: OWLAnnotationPropertyDomainAxiom => renderer.visit(a)
          case a: OWLHasKeyAxiom => renderer.visit(a)
          case a: OWLTransitiveObjectPropertyAxiom => renderer.visit(a)
          case a: OWLSymmetricObjectPropertyAxiom => renderer.visit(a)
          case a: OWLSubClassOfAxiom => renderer.visit(a)
          case a: OWLSameIndividualAxiom => renderer.visit(a)
          case a: OWLReflexiveObjectPropertyAxiom => renderer.visit(a)
          case a: OWLSubObjectPropertyOfAxiom => renderer.visit(a)
          case a: OWLObjectPropertyRangeAxiom => renderer.visit(a)
          case a: OWLObjectPropertyDomainAxiom => renderer.visit(a)
          case a: OWLSubPropertyChainOfAxiom => renderer.visit(a)
          case a: OWLObjectPropertyAssertionAxiom => renderer.visit(a)
          case a: OWLNegativeObjectPropertyAssertionAxiom => renderer.visit(a)
          case a: OWLNegativeDataPropertyAssertionAxiom => renderer.visit(a)
          case a: OWLIrreflexiveObjectPropertyAxiom => renderer.visit(a)
          case a: OWLInverseObjectPropertiesAxiom => renderer.visit(a)
          case a: OWLInverseFunctionalObjectPropertyAxiom => renderer.visit(a)
          case a: OWLFunctionalObjectPropertyAxiom => renderer.visit(a)
          case a: OWLFunctionalDataPropertyAxiom => renderer.visit(a)
          case a: OWLEquivalentObjectPropertiesAxiom => renderer.visit(a)
          case a: OWLEquivalentDataPropertiesAxiom => renderer.visit(a)
          case a: OWLEquivalentClassesAxiom => renderer.visit(a)
          case a: OWLAnnotationAssertionAxiom => renderer.visit(a)
          case a: OWLDisjointUnionAxiom => renderer.visit(a)
          case a: OWLDisjointObjectPropertiesAxiom => renderer.visit(a)
          case a: OWLDisjointDataPropertiesAxiom => renderer.visit(a)
          case a: OWLDisjointClassesAxiom => renderer.visit(a)
          case a: OWLDifferentIndividualsAxiom => renderer.visit(a)
          case a: OWLDeclarationAxiom => renderer.visit(a)
          case a: OWLSubDataPropertyOfAxiom => renderer.visit(a)
          case a: OWLDataPropertyRangeAxiom => renderer.visit(a)
          case a: OWLDataPropertyDomainAxiom => renderer.visit(a)
          case a: OWLDataPropertyAssertionAxiom => renderer.visit(a)
          case a: OWLClassAssertionAxiom => renderer.visit(a)
          case a: OWLAsymmetricObjectPropertyAxiom => renderer.visit(a)
          case _ => throw new RuntimeException(s"Unhandled axiom type ${axiom.getClass.getName}")
        }
        buffPrintWriter.flush()

        os.toString("UTF-8")
      })
    } else {
      Iterator()
    }).saveAsTextFile(filePath)
}
