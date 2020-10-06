package net.sansa_stack.owl.common.parsing

import java.io._
import java.util

import org.apache.jena.rdf.model.{Model, ModelFactory, Statement}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.util.OWLAPIStreamUtils.asList
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import scala.collection.immutable.Stream
import scala.compat.java8.StreamConverters._
import scala.util.matching.Regex


/**
  * Object containing several constants used by the RDFXMLSyntaxParsing
  * trait and the RDFXMLSyntaxExpressionBuilder
  */

object RDFXMLSyntaxParsing {
  /** marker used to store the prefix for the default namespace */
  val _empty = "_EMPTY_"

  val prefixPattern: Regex = "xmlns\\s*:\\s*[a-zA-Z][a-zA-Z0-9_]*\\s*=\\s*([\"'])(?:(?=(\\\\?))\\2.)*?\\1".r
  val ontologyPattern: Regex = "Ontology\\(<(.*)>".r
}

/**
  * Trait to support the parsing of input OWL files in RDFXML syntax.
  * This trait mainly defines how to make axioms from input RDFXML syntax expressions.
  */

trait RDFXMLSyntaxParsing {

  private def man = OWLManager.createOWLOntologyManager()

  private def dataFactory = man.getOWLDataFactory

  private val logger: Logger = LoggerFactory.getLogger(classOf[RDFXMLSyntaxExpressionBuilder])

  def parseRecord(record: String, prefixes: String): util.ArrayList[OWLAxiom] = {
    val owlAxioms = new util.ArrayList[OWLAxiom]()

    val model = ModelFactory.createDefaultModel()
    val modelText = "<?xml version=\"1.0\" encoding=\"utf-8\" ?> \n" + prefixes + record + "</rdf:RDF>"
    model.read(new ByteArrayInputStream(modelText.getBytes()), "http://swat.cse.lehigh.edu")

    val iter = model.listStatements()

    while (iter.hasNext) {
      val stmt = iter.next()
      val axiomSet = makeAxiom(stmt).toList.asJavaCollection
      owlAxioms.addAll(axiomSet)
    }

    owlAxioms
  }

  /**
    * Builds a snippet conforming to the RDF/XML syntax which then can
    * be parsed by the OWL API RDF/XML syntax parser.
    * A single expression, e.g.
    *
    * <rdf:RDF
    * xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    * xmlns:owl="http://www.w3.org/2002/07/owl#">
    * <owl:Class rdf:about="http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person"/>
    * </rdf:RDF>
    *
    * then we will get
    * Declaration(Class("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person"))
    *
    * @param st A Statement containing an expression in RDFXML syntax
    * @return The set of axioms corresponding to the expression.
    */
  def makeAxiom(st: Statement): Set[OWLAxiom] = {

    val model = ModelFactory.createDefaultModel
    model.add(st)

    val ontology: OWLOntology = getOWLOntology(model)
    val axiomSet: Set[OWLAxiom] = ontology.axioms().toScala[Stream].toSet

    axiomSet
  }

  /**
    * To get the corresponding OWLOntology of a certain model
    *
    * @param model Model
    * @return OWLOntology corresponding to that model
    */
  def getOWLOntology(model: Model): OWLOntology = {
    val in = new PipedInputStream
    val o = new PipedOutputStream(in)

    new Thread(new Runnable() {
      def run(): Unit = {
        model.write(o, "RDF/XML-ABBREV")
        //     model.write(System.out, "RDF/XML")

        try {
          o.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }).start()

    val ontology: OWLOntology = man.loadOntologyFromOntologyDocument(in)

    ontology
  }

  /**
    * Handler for wrong domain axioms
    * OWLDataPropertyDomain and OWLObjectPropertyDomain
    * converted wrongly to OWLAnnotationPropertyDomain
    */
  private def fixWrongPropertyDomainAxioms(
                                            sc: SparkContext,
                                            propertyDomainAxioms: RDD[OWLAxiom],
                                            dataPropertiesBC: Broadcast[Array[IRI]],
                                            objPropertiesBC: Broadcast[Array[IRI]]):
      RDD[OWLAxiom] = {

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
  private def fixWrongPropertySubclassAxioms(
                                              sc: SparkContext,
                                              allSubAnnotationPropertyAxioms: RDD[OWLAxiom],
                                              dataPropertiesBC: Broadcast[Array[IRI]],
                                              objPropertiesBC: Broadcast[Array[IRI]]):
      RDD[OWLAxiom] = {

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

  def refineOWLAxioms(sc: SparkContext, axiomsRDD: RDD[OWLAxiom]): RDD[OWLAxiom] = {
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

    val dd = dataProperties.collect()
    val oo = objectProperties.collect()

    val dataPropertiesBC = sc.broadcast(dd)
    val objPropertiesBC = sc.broadcast(oo)

    val annotationPropertyDomainAxioms: RDD[OWLAxiom] =
      axiomsRDD
        .filter(axiom => axiom.getAxiomType.equals(AxiomType.ANNOTATION_PROPERTY_DOMAIN))

    val correctedPropertyDomainAxioms =
      fixWrongPropertyDomainAxioms(sc, annotationPropertyDomainAxioms, dataPropertiesBC, objPropertiesBC)

    val subAnnotationPropertyAxioms =
      axiomsRDD
        .filter(axiom => axiom.getAxiomType.equals(AxiomType.SUB_ANNOTATION_PROPERTY_OF))

    val correctedPropertySubclassAxioms =
      fixWrongPropertySubclassAxioms(sc, subAnnotationPropertyAxioms, dataPropertiesBC, objPropertiesBC)

    val differenceRDD = axiomsRDD
      .subtract(annotationPropertyDomainAxioms)
      .subtract(subAnnotationPropertyAxioms)

    val correctRDD = sc.union(
      differenceRDD,
      correctedPropertyDomainAxioms,
      correctedPropertySubclassAxioms)

    correctRDD
  }
}


/**
  * Trait to support the parsing of prefixes from expressions given in
  * RFD/XML syntax.
  */
trait RDFXMLSyntaxPrefixParsing {

  /**
    * Parses the prefix declaration of a namespace URI and returns the
    * pair (prefix, namespace URI)
    *
    * @param line Sth like
    *             xmlns:="http://swat.cse.lehigh.edu/onto/univ-bench.owl#"
    *             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    */
  def parsePrefix(line: String): (String, String) = {
    var prefix, uri: String = null

    line.trim match {
      case RDFXMLSyntaxParsing.prefixPattern(p, u) =>
        prefix = p
        uri = u
    }

    if (prefix.isEmpty) prefix = RDFXMLSyntaxParsing._empty

    (prefix, uri)
  }

  def isPrefix(expression: String): Boolean = {
    RDFXMLSyntaxParsing.prefixPattern.pattern.matcher(expression).matches()
  }
}


class RDFXMLSyntaxExpressionBuilder (val prefixes: Map[String, String]) extends Serializable {

   def clean(expression: String): String = {
     var trimmedExpr = expression.trim

     /**
       * Throw away expressions that are of no use for further processing:
       * 1) empty lines
       * 2) first line of the xml file
       * 3) the closing, </rdf:RDF> tag
       * 4) prefix declarations
       */
     val discardExpression: Boolean =
       trimmedExpr.isEmpty  || // 1)
         trimmedExpr.toLowerCase.startsWith("<?xml") ||  // 2)
         trimmedExpr.toLowerCase.startsWith("</rdf:rdf") ||  // 3)
         trimmedExpr.toLowerCase.startsWith("<rdf:rdf") // 4)

     if (discardExpression) {
       null

     } else {
       // Expand prefix abbreviations: foo:Bar --> http://foo.com/somePath#Bar
       for (prefix <- prefixes.keys) {
         val p = prefix + ":"

         if (trimmedExpr.contains(p)) {
           val v: String = "<" + prefixes(prefix)

           val localPartPattern = (p + "([a-zA-Z][0-9a-zA-Z_-]*)\\b").r

           // Append ">" to all matched local parts: "foo:car" --> "foo:car>"
           trimmedExpr = localPartPattern.replaceAllIn(trimmedExpr, m => s"${m.matched}>")

           // Expand prefix: "foo:car>" --> "http://foo.org/res#car>"
           trimmedExpr = trimmedExpr.replace(p.toCharArray, v.toCharArray)
         }
       }

       // Handle default prefix e.g. :Bar --> http://foo.com/defaultPath#Bar
       val emptyPrefixPattern = ":[^/][a-zA-Z][0-9a-zA-Z_-]*".r
       val v: String = "<" + prefixes(RDFXMLSyntaxParsing._empty)

       if (prefixes.contains(RDFXMLSyntaxParsing._empty)) {
         emptyPrefixPattern
           .findAllIn(trimmedExpr)
           .foreach(hit => {
            val full = hit.replace(":".toCharArray, v.toCharArray)
            trimmedExpr = trimmedExpr.replace(hit, full + ">")
           })
       }

       trimmedExpr
     }
   }
 }
