package net.sansa_stack.owl.common.parsing

import scala.compat.java8.StreamConverters._

import com.typesafe.scalalogging.Logger
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.OWLXMLDocumentFormat
import org.semanticweb.owlapi.io.StringDocumentSource
import org.semanticweb.owlapi.model._

  /**
    * Singleton instance of type OWLXMLSyntaxParsing
   */

object OWLXMLSyntaxParsing extends Serializable {

    private val logger = Logger(classOf[OWLXMLSyntaxParsing])

    private def manager = OWLManager.createOWLOntologyManager()

    // map to define owlXml schema syntax pattern
    var OWLXMLSyntaxPattern: Map[String, Map[String, String]] = Map(
      "versionPattern" -> Map(
        "beginTag" -> "<?xml",
        "endTag" -> "?>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "prefixPattern" -> Map(
        "beginTag" -> "<rdf:RDF",
        "endTag" -> ">"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "owlClassPattern" -> Map(
        "beginTag" -> "<owl:Class",
        "endTag" -> "</owl:Class>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "owlClassPattern2" -> Map(
        "beginTag" -> "<owl:Class",
        "endTag" -> "/>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "owlDataTypePropertyPattern" -> Map(
        "beginTag" -> "<owl:DatatypeProperty",
        "endTag" -> "</owl:DatatypeProperty>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
        "owlDataTypePropertyPattern2" -> Map(
          "beginTag" -> "<owl:DatatypeProperty",
          "endTag" -> "/>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "owlObjectPropertyPattern" -> Map(
        "beginTag" -> "<owl:ObjectProperty",
        "endTag" -> "</owl:ObjectProperty>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "owlObjectPropertyPattern2" -> Map(
        "beginTag" -> "<owl:ObjectProperty",
        "endTag" -> "/>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "owlNamedIndividualPattern" -> Map(
        "beginTag" -> "<owl:NamedIndividual",
        "endTag" -> "</owl:NamedIndividual>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "owlAnnotationPropertyPattern" -> Map(
        "beginTag" -> "<owl:AnnotationProperty",
        "endTag" -> "</owl:AnnotationProperty>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "owlAnnotationPropertyPattern2" -> Map(
        "beginTag" -> "<owl:AnnotationProperty",
        "endTag" -> "/>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "owlAxiomPattern" -> Map(
        "beginTag" -> "<owl:Axiom",
        "endTag" -> "</owl:Axiom>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "TransitivePropertyPattern" -> Map(
        "beginTag" -> "<owl:TransitiveProperty",
        "endTag" -> "</owl:TransitiveProperty>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "rdfsDatatypePattern" -> Map(
        "beginTag" -> "<rdfs:Datatype",
        "endTag" -> ">"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "rdfDescription" -> Map(
        "beginTag" -> "<rdf:Description",
        "endTag" -> "</rdf:Description>"
      ).withDefaultValue("Pattern for begin and end tags not found"),
      "owlRestriction" -> Map(
        "beginTag" -> "<owl:Restriction",
        "endTag" -> "</owl:Restriction>"
      ).withDefaultValue("Pattern for begin and end tags not found")
    )


    /** definition to build Owl Axioms out of expressions
      *
      * @param xmlString    xmlVersion string
      * @param prefixString owlxml prefix string
      * @param expression   owl expressions of owlxml syntax
      * @return A set consisting OWL Axioms built out of owl expressions
      */

    def makeAxiom(xmlString: String, prefixString: String, expression: String): Set[OWLAxiom] = {

      // compose a string consisting of xml version, owlXml prefixes, owlXml expressions
      val axiomString = xmlString + "\n" + prefixString + "\n" + expression + "\n" + "</rdf:RDF>"

      // load an ontology from file
      val ontology = try {
        // load an ontology from an input stream
        manager.loadOntologyFromOntologyDocument(new StringDocumentSource(axiomString))
      } catch {
        case e: Exception => null
      }

      val extractedAxioms = if (ontology != null) {

        // get the ontology format
        val format = manager.getOntologyFormat(ontology)
        val owlXmlFormat = new OWLXMLDocumentFormat

        // copy all the prefixes from OWL document format to OWL XML format
        if (format != null && format.isPrefixOWLDocumentFormat) {
          owlXmlFormat.copyPrefixesFrom(format.asPrefixOWLDocumentFormat)
        }

        // get axioms from the loaded ontology
        val axioms: Set[OWLAxiom] = ontology.axioms().toScala[Set]
        axioms

      } else {
        // logger.warn("No ontology was created for expression \n" + expression + "\n")
        null
      }
      extractedAxioms
    }

    /**
      * Trait to support the parsing of input OWL files in OWLXML syntax.
      * This trait mainly defines how to make axioms from input OWLXML
      * syntax expressions.
      */

    trait OWLXMLSyntaxParsing extends Serializable {
    }
  }
