package net.sansa_stack.owl.common.parsing

import java.io._
import java.util.ArrayList

import scala.collection.immutable.Stream
import scala.compat.java8.StreamConverters._
import scala.util.matching.Regex

import collection.JavaConverters._
import org.apache.jena.rdf.model.{Model, ModelFactory, Statement}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.util.OWLAPIStreamUtils.asList
import org.slf4j.Logger
import org.slf4j.LoggerFactory


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

  val LOGGER: Logger = LoggerFactory.getLogger(classOf[RDFXMLSyntaxExpressionBuilder])
  // private var obj = OWLObject

  // private def ontConf = man.getOntologyLoaderConfiguration

  def RecordParse(record: String, prefixes: String): ArrayList[OWLAxiom] = {

    var OWLAxioms = new ArrayList[OWLAxiom]()

    val model = ModelFactory.createDefaultModel()
    val modelText = "<?xml version=\"1.0\" encoding=\"utf-8\" ?> \n" + prefixes + record + "</rdf:RDF>"
    model.read(new ByteArrayInputStream(modelText.getBytes()), "http://swat.cse.lehigh.edu")

    val iter = model.listStatements()

    while (iter.hasNext()) {
      val stmt = iter.next()
      val axiomSet = makeAxiom(stmt).toList.asJavaCollection
      OWLAxioms.addAll(axiomSet)
    }

    OWLAxioms
  }

  /**
    * Builds a snippet conforming to the RDFXML syntax which then can
    * be parsed by the OWLAPI RDFXML syntax parser.
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


  def refineOWLAxioms(sc: SparkContext, axiomsRDD: RDD[OWLAxiom]): RDD[OWLAxiom] = {

    axiomsRDD.cache()

  /**
    *  Case 1: Handler for wrong domain axioms
    *  OWLDataPropertyDomain and OWLObjectPropertyDomain
    *  converted wrong to OWLAnnotationPropertyDomain
   */

    val declaration = extractAxiom(axiomsRDD, AxiomType.DECLARATION)
      .asInstanceOf[RDD[OWLDeclarationAxiom]]

    val dataProperties = declaration.filter(a => a.getEntity.isOWLDataProperty)
      .map(a => a.getEntity.getIRI)

    val objectProperties = declaration.filter(a => a.getEntity.isOWLObjectProperty)
      .map(a => a.getEntity.getIRI)

    val dd = dataProperties.collect()
    val oo = objectProperties.collect()

    val dataBC = sc.broadcast(dd)
    val objBC = sc.broadcast(oo)

    val annDomain = extractAxiom(axiomsRDD, AxiomType.ANNOTATION_PROPERTY_DOMAIN)

    val domainTransform = annDomain.asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .map { a =>

        val prop = a.getProperty.getIRI
        val domain = a.getDomain

        // Annotation property domain axiom turned to object property domain after parsing.
        if (objBC.value.contains(prop)) {

          val c = dataFactory.getOWLClass(domain)
          val p = dataFactory.getOWLObjectProperty(prop.toString)
          val obj = dataFactory.getOWLObjectPropertyDomainAxiom(p, c, asList(a.annotations))

          obj

        } else if (dataBC.value.contains(prop)) {

          // Annotation property domain axiom turned to object property domain after parsing.
          val c = dataFactory.getOWLClass(domain)
          val p = dataFactory.getOWLDataProperty(prop.toString)
          val obj = dataFactory.getOWLDataPropertyDomainAxiom(p, c, asList(a.annotations))

          obj

        } else {

          val obj = dataFactory
            .getOWLAnnotationPropertyDomainAxiom(a.getProperty.asOWLAnnotationProperty, domain, asList(a.annotations))
            .asInstanceOf[OWLAxiom]
          obj

        }
      }

  /**
    * Case 2: Handler for wrong subPropertyOf axioms
    * OWLSubPropertyOf converted wrong to OWLSubAnnotationPropertyOf
    * instead of OWLSubDataPropertyOf or OWLSubObjectPropertyOf
    */

    val subAnnotationProperty = extractAxiom(axiomsRDD, AxiomType.SUB_ANNOTATION_PROPERTY_OF)

    val subPropertyTransform = subAnnotationProperty.asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .map { a =>

        val subProperty = a.getSubProperty.getIRI
        val supProperty = a.getSuperProperty.getIRI


        // SubAnnotationPropertyOf axiom turned to SubObjectPropertyOf after parsing.
        if (objBC.value.contains(subProperty)) {

          val sub = dataFactory.getOWLObjectProperty(subProperty.toString)
          val sup = dataFactory.getOWLObjectProperty(supProperty.toString)
          val obj = dataFactory.getOWLSubObjectPropertyOfAxiom(sub, sup, asList(a.annotations))

          obj

        } else if (dataBC.value.contains(subProperty)) {

          // SubAnnotationPropertyOf axiom turned to SubDataPropertyOf after parsing.
          val sub = dataFactory.getOWLDataProperty(subProperty.toString)
          val sup = dataFactory.getOWLDataProperty(supProperty.toString)
          val obj = dataFactory.getOWLSubDataPropertyOfAxiom(sub, sup, asList(a.annotations))

          obj

        } else {

          val obj = dataFactory
              .getOWLSubAnnotationPropertyOfAxiom(a.getSubProperty.asOWLAnnotationProperty,
                                                  a.getSuperProperty.asOWLAnnotationProperty(),
                                                  asList(a.annotations))
              .asInstanceOf[OWLAxiom]
          obj

        }
      }

    val differenceRDD = axiomsRDD.subtract(annDomain).subtract(subAnnotationProperty)
    val correctRDD = sc.union(differenceRDD, domainTransform, subPropertyTransform)

    correctRDD.foreach(println(_))

    correctRDD
  }


  def extractAxiom(axiom: RDD[OWLAxiom], T: AxiomType[_]): RDD[OWLAxiom] = {
    axiom.filter(a => a.getAxiomType.equals(T))
  }
}

/**
  * Trait to support the parsing of prefixes from expressions given in
  * RFDXML syntax.
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
    var temp: String = line

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

     /* Throw away expressions that are of no use for further processing:
      * 1) empty lines
      * 2) first line of the xml file
      * 3) the closing, </rdf:RDF> tag
      * 4) prefix declaration
      */
     val discardExpression: Boolean =
       trimmedExpr.isEmpty  || // 1)
         trimmedExpr.startsWith("<?xml") ||  // 2)
         trimmedExpr.startsWith("</rdf:RDF") ||  // 3)
         trimmedExpr.startsWith("<rdf:RDF") // 4)

     if (discardExpression) {
       null

     } else {
       // Expand prefix abbreviations: foo:Bar --> http://foo.com/somePath#Bar
       for (prefix <- prefixes.keys) {
         val p = prefix + ":"

         if (trimmedExpr.contains(p)) {
           val v: String = "<" + prefixes.get(prefix).get

           val pattern = (p + "([a-zA-Z][0-9a-zA-Z_-]*)\\b").r

           // Append ">" to all matched local parts: "foo:car" --> "foo:car>"
           trimmedExpr = pattern.replaceAllIn(trimmedExpr, m => s"${m.matched}>")
           // Expand prefix: "foo:car>" --> "http://foo.org/res#car>"
           trimmedExpr = trimmedExpr.replace(p.toCharArray, v.toCharArray)
         }
       }

       // handle default prefix e.g. :Bar --> http://foo.com/defaultPath#Bar

       val pattern = ":[^/][a-zA-Z][0-9a-zA-Z_-]*".r
       val v: String = "<" + prefixes.get(FunctionalSyntaxParsing._empty).get

       if (prefixes.contains(RDFXMLSyntaxParsing._empty)) {
         pattern.findAllIn(trimmedExpr).foreach(hit => {
           val full = hit.replace(":".toCharArray, v.toCharArray)
           trimmedExpr = trimmedExpr.replace(hit, full + ">")
         })
       }

       trimmedExpr
     }
   }
 }
