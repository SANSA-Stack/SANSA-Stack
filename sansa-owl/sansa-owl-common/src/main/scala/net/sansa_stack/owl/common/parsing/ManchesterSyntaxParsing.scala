package net.sansa_stack.owl.common.parsing

import scala.collection.JavaConverters._

import com.typesafe.scalalogging.Logger
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.expression.OWLEntityChecker
import org.semanticweb.owlapi.manchestersyntax.parser.ManchesterOWLSyntax
import org.semanticweb.owlapi.model.{OWLAnnotationProperty, OWLAxiom, OWLClass, OWLDataProperty, OWLDatatype, OWLLogicalEntity, OWLNamedIndividual, OWLObjectProperty, OWLOntology}
import org.semanticweb.owlapi.util.mansyntax.ManchesterOWLSyntaxParser
import org.semanticweb.owlapi.vocab.XSDVocabulary



/**
  * This class is used to support the parsing of single frames in Manchester OWL
  * syntax. The main purpose is to always return a requested OWL entity without
  * actually checking whether it was defined beforehand. This is required since
  * the Mancherster OWL syntax parser has to deal with single frames without
  * having access to a whole ontology.
  * Another purpose of this entity checker is to expand namespace abbreviations.
  *
  * @param defaultPrefix String containing the default namespace which will be
  *                      used to expand non-full URIs
  */
class FakeEntityChecker(defaultPrefix: String) extends OWLEntityChecker {
  val dataFactory = OWLManager.getOWLDataFactory

  private def clean(name: String, factory: String => OWLLogicalEntity) = {
    if (name.startsWith("<")) {
      // remove angle brackets as in <http://ex.com/bar#Cls1>
      factory(name.replace("<", "").replace(">", ""))
    } else {
      if (name == "|EOF|") {
        // TODO: don't know whether this works at all
        // It happened that a URI <http://ex.com/default#|EOF|> was created.
        // After some changes I could not reproduce it so might be it won't occur
        // again.
        null
      } else {
        // for cases like
        //   AnnotationProperty: description
        // this will return sth like <http://ex.com/default#description>
        factory(defaultPrefix + name)
      }
    }
  }

  override def getOWLIndividual(name: String): OWLNamedIndividual =
    clean(name, dataFactory.getOWLNamedIndividual).asOWLNamedIndividual()

  override def getOWLDataProperty(name: String): OWLDataProperty =
    clean(name, dataFactory.getOWLDataProperty).asOWLDataProperty()

  override def getOWLDatatype(name: String): OWLDatatype = {
    if (!name.startsWith("<")) {
      val tmp = name.split(":")
      val ns = tmp(0)

      ns match {
        case "xsd" => dataFactory.getOWLDatatype(XSDVocabulary.parseShortName(name).getIRI)
        case _ => dataFactory.getOWLDatatype(defaultPrefix + name)
      }

    } else {
      dataFactory.getOWLDatatype(name.replace("<", "").replace(">", ""))
    }
  }

  override def getOWLObjectProperty(name: String): OWLObjectProperty =
    clean(name, dataFactory.getOWLObjectProperty).asOWLObjectProperty()

  override def getOWLClass(name: String): OWLClass =
    clean(name, dataFactory.getOWLClass).asOWLClass()

  override def getOWLAnnotationProperty(name: String): OWLAnnotationProperty =
    clean(name, dataFactory.getOWLAnnotationProperty).asOWLAnnotationProperty()
}


/**
  * Object to store any kinds of constants
  */
object ManchesterSyntaxParsing {
  /** marker used to store the prefix for the default namespace */
  val _empty = "_EMPTY_"
  val dummyURI = "http://sansa-stack.net/dummy"
  // TODO: refine
  val prefixPattern = "Prefix\\:\\s*([a-zA-Z]*)\\:\\s*<(.*)>".r
  val ontologyPattern = "Ontology\\:\\s*<(.*)>".r
  val keywords = ManchesterOWLSyntax.values().filter(_.isAxiomKeyword) ++
    ManchesterOWLSyntax.values().filter(_.isClassExpressionConnectiveKeyword) ++
    ManchesterOWLSyntax.values().filter(_.isClassExpressionQuantiferKeyword)
}


/**
  * Trait to support the parsing of input files in Manchester OWL syntax.
  * The main functionality is to create OWL axioms from a single Manchester OWL
  * syntax frame. For now we simply call the OWL API Manchester OWL syntax
  * parser for that. Unfortunately, there are quite a few cases that cannot be
  * handled by the OWL API Manchester OWL syntax parser, now.
  */
trait ManchesterSyntaxParsing {
  private val logger = Logger(classOf[ManchesterSyntaxParsing])
  var parser: ManchesterOWLSyntaxParser = null
  var ont: OWLOntology = null
  val encounteredErrMsgPattern = "Encountered (.*) at line".r
  val expectedPatternErrMsgPattern = "Expected one of:\\s(.*)$".r

//  @throws(classOf[OWLParserException])
//  def makeAxioms(frame: String, defaultPrefix: String): Set[OWLAxiom] = {
//    val parser = OWLManager.createManchesterParser()
//    parser.setDefaultOntology(OWLManager.createOWLOntologyManager().createOntology(IRI.create(defaultPrefix)))
//    parser.setOWLEntityChecker(new FakeEntityChecker(defaultPrefix))
//    parser.setStringToParse(frame)
//
//    parser.parseFrames().asScala.map(_.getAxiom).toSet
//  }

  def makeAxioms(frame: String, defaultPrefix: String): Set[OWLAxiom] = {
    val parser = ManchesterParser
    parser.prefixes.put("", defaultPrefix)

    parser.parseFrame(frame).toSet
  }
}


/**
  * Trait to support the parsing of prefix definitions in Manchester OWL syntax
  */
trait ManchesterSyntaxPrefixParsing {
  def parsePrefix(prefixFrame: String): (String, String) = {
    var prefix, uri: String = null

    prefixFrame.trim match {
      case ManchesterSyntaxParsing.prefixPattern(p, u) =>
        prefix = p
        uri = u
    }

    if (prefix.isEmpty) prefix = ManchesterSyntaxParsing._empty

    (prefix, uri)
  }

  def isPrefixDeclaration(frame: String): Boolean = {
    ManchesterSyntaxParsing.prefixPattern.pattern.matcher(frame).matches()
  }
}


/**
  * Class that supports the clean-up of Manchester OWL syntax frames.
  * An input frame will be
  *
  * - discarded if it is a prefix declaration, an ontology definition or in
  *   case it is empty
  * - replaced by the same frame with all short URIs replaced by full URIs
  *   (except short URIs of the default namespace which will be replaced later
  *   (mainly due to the difficulty to parse them right here))
  * - trimmed, i.e. all leading and trailing whitespace will be removed
  *
  * @param prefixes A map of all known prefix declarations used for replacing
  *                 short URIs
  */
class ManchesterSyntaxExpressionBuilder(val prefixes: Map[String, String]) extends Serializable {
  def clean(frame: String): String = {
    var trimmedFrame = frame.trim
    val discardFrame: Boolean =
      trimmedFrame.isEmpty ||
      trimmedFrame.startsWith("Prefix") ||
      trimmedFrame.startsWith("Ontology") ||
      trimmedFrame.startsWith("<http")

    if (discardFrame) {
      null

    } else {
      for (prefix <- prefixes.keys) {
        val p = prefix + ":"

        if (trimmedFrame.contains(p)) {
          val v: String = "<" + prefixes.get(prefix).get
          val pattern = (p + "([a-zA-Z][0-9a-zA-Z_-]*)").r

          pattern.findAllIn(trimmedFrame) foreach (hit => {
            if (!trimmedFrame.contains(hit + ">")) {
              trimmedFrame = trimmedFrame.replace(hit, hit + ">")
            }
          })
          trimmedFrame = trimmedFrame.replace(p.toCharArray, v.toCharArray)
        }
      }

      trimmedFrame
    }
  }
}
