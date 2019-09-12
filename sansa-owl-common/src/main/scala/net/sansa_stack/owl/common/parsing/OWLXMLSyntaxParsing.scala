package net.sansa_stack.owl.common.parsing

import scala.compat.java8.StreamConverters._
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.OWLXMLDocumentFormat
import org.semanticweb.owlapi.io.StringDocumentSource
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.util.OWLAPIStreamUtils.asList

import net.sansa_stack.owl.spark.rdd.{OWLAxiomsRDD, OWLExpressionsRDD}


  /**
    * Singleton instance of type OWLXMLSyntaxParsing
    */

object OWLXMLSyntaxParsing {

  // map to define OWLXML syntax pattern
  val OWLXMLSyntaxPattern: Map[String, Map[String, String]] = Map(
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
//    ).withDefaultValue("Pattern for begin and end tags not found"),
//    "rdfDescritionPattern" -> Map(
//      "beginTag" -> "<rdf:Description",
//      "endTag" -> "</rdf:Description>"
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
    "rdfsDatatypePattern" -> Map(
      "beginTag" -> "<rdfs:Datatype",
      "endTag" -> ">"
    ).withDefaultValue("Pattern for begin and end tags not found")
  )

}

/**
  * Trait to support the parsing of input OWL files in OWLXML syntax.
  * This trait mainly defines how to make axioms from input OWLXML
  * syntax expressions.
  */

trait OWLXMLSyntaxParsing {

  private val logger = Logger(classOf[OWLXMLSyntaxParsing])
  private def manager = OWLManager.createOWLOntologyManager()
  private def dataFactory = manager.getOWLDataFactory


  //  private def parser = new OWLXMLParserFactory().createParser()
 // private def ontConf = manager.getOntologyLoaderConfiguration

  /** definition to build Owl Axioms out of expressions
    * @param xmlString xmlVersion string
    * @param prefixString owlxml prefix string
    * @param expression owl expressions of owlxml syntax
    * @return A set consisting OWL Axioms built out of owl expressions
    */

  def makeAxiom(xmlString: String, prefixString: String, expression: String): Set[OWLAxiom] = {

    // compose a string consisting of xml version, owlxml prefixes, owlxml expressions
    val axiomString = xmlString + "\n" + prefixString + "\n" + expression + "\n" + "</rdf:RDF>"

    // load an ontology from file
    val ontology = try {

      val name = "org.semanticweb.owlapi.rio.RioTrixParserFactory"
      manager.getOntologyConfigurator.withBannedParsers(name)
      // load an ontology from an input stream
      manager.loadOntologyFromOntologyDocument(new StringDocumentSource(axiomString))
    } catch {
      case e: OWLOntologyCreationException => null
    }

    val extractedAxioms = if (ontology!=null) {

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
   //   logger.warn("No ontology was created for expression \n" + expression + "\n")
      null
    }
    extractedAxioms
  }


  /**
    * Handler for wrong domain axioms
    * OWLDataPropertyDomain and OWLObjectPropertyDomain
    * converted wrongly to OWLAnnotationPropertyDomain
    */
  private def fixWrongPropertyDomainAxioms( sc: SparkContext,
                                            propertyDomainAxioms: RDD[OWLAxiom],
                                            dataPropertiesBC: Broadcast[Array[IRI]],
                                            objPropertiesBC: Broadcast[Array[IRI]]): OWLAxiomsRDD = {

    val correctedPropertyDomainAxioms: RDD[OWLAxiom] =
      propertyDomainAxioms
        .asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
        .map { annPropDomAxiom =>

          val prop = annPropDomAxiom.getProperty.getIRI
          val domain = annPropDomAxiom.getDomain

          // Annotation property domain axiom turned into object property domain after parsing.
          if (objPropertiesBC.value.contains(prop)) {

            // FIXME: Case of non-atomic class expressions not handled
            val domainClass = dataFactory.getOWLClass(domain)
            val objProp = dataFactory.getOWLObjectProperty(prop)
            val propertyDomainAxiom =
              dataFactory.getOWLObjectPropertyDomainAxiom(objProp, domainClass, asList(annPropDomAxiom.annotations))

            propertyDomainAxiom.asInstanceOf[OWLAxiom]

          } else if (dataPropertiesBC.value.contains(prop)) {

            // Annotation property domain axiom turned into data property domain after parsing.
            // FIXME: Case of non-atomic class expressions not handled
            val domainClass = dataFactory.getOWLClass(domain)
            val dataProp = dataFactory.getOWLDataProperty(prop.toString)
            val propertyDomainAxiom =
              dataFactory.getOWLDataPropertyDomainAxiom(dataProp, domainClass, asList(annPropDomAxiom.annotations))

            propertyDomainAxiom.asInstanceOf[OWLAxiom]

          } else {

            annPropDomAxiom
          }
        }

    correctedPropertyDomainAxioms
  }

  /**
    * Handler for wrong subPropertyOf axioms
    * OWLSubPropertyOf converted wrongly to OWLSubAnnotationPropertyOf
    * instead of OWLSubDataPropertyOf or OWLSubObjectPropertyOf
    */
  private def fixWrongSubPropertyAxioms( sc: SparkContext,
                                              allSubAnnotationPropertyAxioms: RDD[OWLAxiom],
                                              dataPropertiesBC: Broadcast[Array[IRI]],
                                              objPropertiesBC: Broadcast[Array[IRI]]): OWLAxiomsRDD = {

    val correctedSubPropertyAxioms =
      allSubAnnotationPropertyAxioms
        .asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
        .map { subAnnPropAxiom =>

          val subProperty = subAnnPropAxiom.getSubProperty.getIRI
          val supProperty = subAnnPropAxiom.getSuperProperty.getIRI

          // SubAnnotationPropertyOf axiom turned into SubObjectPropertyOf after parsing.
          if (objPropertiesBC.value.contains(subProperty)) {

            val subObjProperty = dataFactory.getOWLObjectProperty(subProperty)
            val superObjProperty = dataFactory.getOWLObjectProperty(supProperty)
            val subObjPropertyAxiom =
              dataFactory.getOWLSubObjectPropertyOfAxiom(subObjProperty, superObjProperty, asList(subAnnPropAxiom.annotations))

            subObjPropertyAxiom.asInstanceOf[OWLAxiom]

          } else if (dataPropertiesBC.value.contains(subProperty)) {

            // SubAnnotationPropertyOf axiom turned to SubDataPropertyOf after parsing.
            val subDataProperty = dataFactory.getOWLDataProperty(subProperty)
            val superDataProperty = dataFactory.getOWLDataProperty(supProperty)
            val subDataPropertyAxiom =
              dataFactory.getOWLSubDataPropertyOfAxiom(subDataProperty, superDataProperty, asList(subAnnPropAxiom.annotations))

            subDataPropertyAxiom.asInstanceOf[OWLAxiom]

          } else {

            subAnnPropAxiom
          }
        }

    correctedSubPropertyAxioms
  }

  /**
    * Handler for wrong Assertion axioms
    * OWLDataPropertyAssertion and OWLObjectPropertyAssertion
    * converted wrongly to OWLAnnotationPropertyAssertion
    */

  def fixWrongPropertyAssertionAxioms (sc: SparkContext,
                                       annotationAssertionAxioms: RDD[OWLAxiom],
                                       dataPropertiesBC: Broadcast[Array[IRI]],
                                       objPropertiesBC: Broadcast[Array[IRI]]): OWLAxiomsRDD = {

    val correctedAnnotationAssertionAxioms =
      annotationAssertionAxioms
        .asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]
        .map { annAssertionAxiom =>

          val annProperty = annAssertionAxiom.getProperty.getIRI

          // ObjectPropertyAssertion axiom turned into AnnotationAssertion after parsing.
          if (objPropertiesBC.value.contains(annProperty)) {
             val annSubject: IRI = annAssertionAxiom.getSubject.asIRI().get()
             val annValue = annAssertionAxiom.getValue.toString

             val objProperty = dataFactory.getOWLObjectProperty(annProperty)
             val individual1 = dataFactory.getOWLNamedIndividual(annSubject)
             val individual2 = dataFactory.getOWLNamedIndividual(annValue)
             val objPropertyAssertionAxiom =
                  dataFactory.getOWLObjectPropertyAssertionAxiom(objProperty, individual1, individual2)

             objPropertyAssertionAxiom.asInstanceOf[OWLAxiom]

          } else if (dataPropertiesBC.value.contains(annProperty)) {

              // dataPropertyAssertion axiom turned into AnnotationAssertion after parsing.

             val annSubject: IRI = annAssertionAxiom.getSubject.asIRI().get()
             val annValue = annAssertionAxiom.getValue.asLiteral().get()

             val dataProperty = dataFactory.getOWLDataProperty(annProperty)
             val individual1 = dataFactory.getOWLNamedIndividual(annSubject)
        //     val dataType = dataFactory.getOWLLiteral(annValue)
             val dataPropertyAssertionAxiom =
                    dataFactory.getOWLDataPropertyAssertionAxiom(dataProperty, individual1, annValue)

             dataPropertyAssertionAxiom.asInstanceOf[OWLAxiom]

          } else {
              annAssertionAxiom
          }
        } // map

    correctedAnnotationAssertionAxioms.asInstanceOf[OWLAxiomsRDD]
  }

  def refineOWLAxioms(sc: SparkContext, axiomsRDD: OWLAxiomsRDD): OWLAxiomsRDD = {
    axiomsRDD.cache()

    val allDeclarations: RDD[OWLDeclarationAxiom] =
      axiomsRDD
        .filter(axiom => axiom.getAxiomType.equals(AxiomType.DECLARATION))
        .asInstanceOf[RDD[OWLDeclarationAxiom]]

    val dataProperties: RDD[IRI] =
      allDeclarations
        .filter(a => a.getEntity.isOWLDataProperty)
        .map(a => a.getEntity.getIRI)

    val objectProperties: RDD[IRI] =
      allDeclarations
        .filter(a => a.getEntity.isOWLObjectProperty)
        .map(a => a.getEntity.getIRI)

    val d = dataProperties.collect()
    val o = objectProperties.collect()

    val dataPropertiesBC = sc.broadcast(d)
    val objPropertiesBC = sc.broadcast(o)

    val annotationPropertyDomainAxioms =
      axiomsRDD
        .filter(axiom => axiom.getAxiomType.equals(AxiomType.ANNOTATION_PROPERTY_DOMAIN))

    val correctPropertyDomainAxioms =
      fixWrongPropertyDomainAxioms(sc, annotationPropertyDomainAxioms, dataPropertiesBC, objPropertiesBC)

    val subAnnotationPropertyAxioms =
      axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.SUB_ANNOTATION_PROPERTY_OF))

    val correctSubPropertyAxioms =
      fixWrongSubPropertyAxioms(sc, subAnnotationPropertyAxioms, dataPropertiesBC, objPropertiesBC)

    val annotationAssertionAxioms = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.ANNOTATION_ASSERTION))

    val correctAssertionAxioms =
      fixWrongPropertyAssertionAxioms(sc, annotationAssertionAxioms, dataPropertiesBC, objPropertiesBC)

//    val equivalentClasses: RDD[OWLEquivalentClassesAxiom] = axiomsRDD
//      .filter(axiom => axiom.getAxiomType.equals(AxiomType.EQUIVALENT_CLASSES) )
//      .asInstanceOf[RDD[OWLEquivalentClassesAxiom]]
//      .filter(eq => eq.getOperandsAsList.get(1).isOWLClass)
//
//    println("\n\n--------- \n Equivalent wrong classes \n")
//    equivalentClasses.foreach(println(_))

    val differenceRDD = axiomsRDD
      .subtract(annotationPropertyDomainAxioms)
      .subtract(subAnnotationPropertyAxioms)
      .subtract(annotationAssertionAxioms)

    val correctRDD = sc.union(
      differenceRDD,
      correctSubPropertyAxioms,
      correctPropertyDomainAxioms,
      correctAssertionAxioms)

    correctRDD
  }
}


//  @throws(classOf[OWLParserException])
//  def makeAxiom(expression: String): OWLAxiom = {
//    val ontStr = "Ontology(<http://example.com/dummy>\n"
//    val axStr = ontStr + expression + "\n)"
//    val ont = manager.createOntology()
//
//    parser.parse(new StringDocumentSource(axStr), ont, ontConf)
//
//    val it = ont.axioms().iterator()
//
//    if (it.hasNext) {
//      it.next()
//    } else {
//      logger.warn("No axiom was created for expression " + expression)
//      null
//    }
//  }

// class OWLXMLSyntaxParsing extends Serializable {
//
// }

class OWLXMLSyntaxExpressionBuilder(spark: SparkSession, filePath: String) {

  // define an empty RDD of generic type
  private var owlRecordRDD : org.apache.spark.rdd.RDD[String] = spark.sparkContext.emptyRDD

  // get xml version string as an RDD
  private val xmlVersionRDD = getRecord(OWLXMLSyntaxParsing.OWLXMLSyntaxPattern("versionPattern"))

  // get owlxml prefix string as an RDD
  private val owlPrefixRDD = getRecord(OWLXMLSyntaxParsing.OWLXMLSyntaxPattern("prefixPattern"))

  // get pattern for begin and end tags for owl expressions to be specified for hadoop stream
  private val owlRecordPatterns = OWLXMLSyntaxParsing.OWLXMLSyntaxPattern
    .filterKeys(_ != "versionPattern").filterKeys(_ != "prefixPattern")

  /**
    * definition to get owl expressions from hadoop stream as RDD[String]
    *
    * @param pattern a Map object consisting of key-value pairs to define begin and end tags
    * @return OWlExpressionRDD
    */

  def getRecord(pattern: Map[String, String]): OWLExpressionsRDD = {

    val config = new JobConf()
    val beginTag: String = pattern("beginTag")
    val endTag: String = pattern("endTag")

    config.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    config.set("stream.recordreader.begin", beginTag) // start Tag
    config.set("stream.recordreader.end", endTag) // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(config, filePath)

    val rawRDD: RDD[(Text, Text)] = spark.sparkContext.hadoopRDD(config,
      classOf[org.apache.hadoop.streaming.StreamInputFormat], // input format
      classOf[org.apache.hadoop.io.Text], // class for the key
      classOf[org.apache.hadoop.io.Text]) // class for the value

    val recordRDD: OWLExpressionsRDD = rawRDD.map { (x) => (x._1.toString) }

    recordRDD
  }

//  /**
//    * definition to get owl expressions for each patterns defined in OWLXMLSyntaxPattern
//    *
//    * @return rdd of owl expression string
//    */
//
//  def getOwlExpressions(): OWLExpressionsRDD = {
//
//    val unionOwlExpressionsRDD = for {
//      (pattern, tags) <- owlRecordPatterns
//    } yield owlRecordRDD.union(getRecord(tags))
//
//    val owlExpressionsRDD = unionOwlExpressionsRDD.reduce(_ union _)
//
//    owlExpressionsRDD
//  }
// }


  /**
    * definition to get owl expressions for each patterns defined in OWLXMLSyntaxPattern
    *
    * @return a tuple consisting of (RDD for xml version, RDD for owlxml prefixes, RDD for owl expressions)
    */

  def getOwlExpressions(): (OWLExpressionsRDD, OWLExpressionsRDD, OWLExpressionsRDD) = {

    val unionOwlExpressionsRDD = for {
      (pattern, tags) <- owlRecordPatterns
    } yield owlRecordRDD.union(getRecord(tags))

    val owlExpressionsRDD = unionOwlExpressionsRDD.reduce(_ union _)

    (xmlVersionRDD, owlPrefixRDD, owlExpressionsRDD)
  }

}
