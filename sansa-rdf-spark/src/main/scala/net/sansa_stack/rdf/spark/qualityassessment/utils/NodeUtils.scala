package net.sansa_stack.rdf.spark.qualityassessment.utils

import java.io.IOException
import java.net.{ HttpURLConnection, MalformedURLException, ProtocolException }
import java.net.URL

import net.sansa_stack.rdf.spark.qualityassessment.utils.DatasetUtils._
import net.sansa_stack.rdf.spark.qualityassessment.vocabularies.DQV
import net.sansa_stack.rdf.spark.utils.StatsPrefixes._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.vocabulary.RDFS
import scala.util.matching.Regex

/**
 * Node Utils.
 * @author Gezim Sejdiu
 */
object NodeUtils extends Serializable {

  /**
   * Checks if a resource ?node is local
   */
  def isInternal(node: Node): Boolean =
    prefixes.exists(prefix =>
      (if (node.isLiteral) node.getLiteralLexicalForm else node.toString()).startsWith(prefix))

  /**
   *  Checks if a resource ?node is local
   */
  def isExternal(node: Node): Boolean =
    !isInternal(node)

  /**
   * Test whether the given literal lexical form is
   * a legal lexical form of this datatype.
   */
  def isLexicalFormCompatibleWithDatatype(node: Node): Boolean =
    node.getLiteralDatatype().isValid(node.getLiteralLexicalForm)

  val isLicenseDefination = new Regex(".*(licensed?|copyrighte?d?).*(under|grante?d?|rights?).*")
  val licenceIndications = Seq(DQV.dqv_description, RDFS.comment, RDFS.label)

  /**
   * Checks if a given [[resource]] contains license statements.
   * License statements : .*(licensed?|copyrighte?d?).*(under|grante?d?|rights?).*
   * @param node the resource to be checked.
   * @return `true` if contains these definition, otherwise `false`.
   */
  def isLicenseStatement(node: Node): Boolean =
    isLicenseDefination.findFirstIn(node.getLiteralLexicalForm).size != 0

  /**
   * Checks if a given [[resource]] contains license indications.
   * License indications : [[http://www.w3.org/ns/dqv#description dqv:description]], [[https://www.w3.org/2000/01/rdf-schema#comment RDFS.comment]], [[https://www.w3.org/2000/01/rdf-schema#label RDFS.label]]
   * @param node the resource to be checked.
   * @return `true` if contains these indications, otherwise `false`.
   */
  def hasLicenceIndications(node: Node): Boolean =
    licenceIndications.contains(node.getURI)

  val licenceAssociated = Seq(DQV.cclicence, DQV.dbolicense, DQV.xhtmllicense, DQV.dclicence,
    DQV.dcrights, DQV.dctlicense, DQV.dbplicence, DQV.doaplicense,
    DQV.dctrights, DQV.schemalicense, "wrcc:license", "sz:license_text")

  /**
   * Checks if a given [[resource]] is license associated.
   * @param node the resource to be checked.
   * @return `true` if contains these definition, otherwise `false`.
   */
  def hasLicenceAssociated(node: Node): Boolean =
    licenceAssociated.contains(node.getURI)

  /**
   * Checks if a resource @node is broken
   */
  def isBroken(node: Node): Boolean = {
    var isBroken = false
    var extUrl: URL = null

    try {
      extUrl = new URL(node.getURI()) // retrieving extUrl
    } catch {
      case e: MalformedURLException =>
        (isBroken = true)
        true
    }

    var urlConn: HttpURLConnection = null
    try {
      urlConn = extUrl.openConnection().asInstanceOf[HttpURLConnection]
    } catch {
      case ioe: IOException =>
        (isBroken = true) // IO Exception
        true
      case e: Exception =>
        (isBroken = true) // General Exception
        true
    }
    try {
      urlConn.setRequestMethod("HEAD")
    } catch {
      case e: ProtocolException =>
        (isBroken = true) // Protocol error
        true
    }

    var responseCode = 0;

    try {
      urlConn.connect();
      responseCode = urlConn.getResponseCode();
    } catch {
      case e: IOException =>
        (isBroken = true) // Not able to retrieve response code
        true
    }

    if (responseCode >= 200 && responseCode < 400) {
      isBroken = false
      false
    } else {
      isBroken = true // Bad response code
      true
    }

    isBroken
  }

  def isHashUri(node: Node): Boolean = node.getURI().indexOf("#") > -1

  def getParentURI(node: Node): String = {

    var parentURI = ""
    if (node.isURI() && node.getURI().toString() != "") {

      val lastSlashIx = node.getURI().lastIndexOf('/')

      if (lastSlashIx > 0) {
        parentURI = node.getURI().substring(0, lastSlashIx)
      } else parentURI = ""
    }
    parentURI
  }

  def checkLiteral(node: Node): String =
    if (node.isLiteral) node.getLiteralLexicalForm else node.toString()
  def isLabeled(node: Node): Boolean =
    (if (node.isLiteral) node.getLiteralLexicalForm else node.toString).contains(RDFS_LABEL)

  def isRDFSClass(node: Node): Boolean =
    (if (node.isLiteral) node.getLiteralLexicalForm else node.toString).contains(RDFS_CLASS)

  def isOWLClass(node: Node): Boolean =
    (if (node.isLiteral) node.getLiteralLexicalForm else node.toString).contains(OWL_CLASS)

  def resourceTooLong(node: Node): Boolean =
    (node.getURI().length() >= shortURIThreshold)

  def hasQueryString(node: Node): Boolean = {
    val uri = node.getURI()
    val qMarkIndex = uri.indexOf("?")
    val hashTagIndex = uri.indexOf("#")

    (qMarkIndex > -1 && (hashTagIndex == -1 || qMarkIndex < hashTagIndex))
  }
}
