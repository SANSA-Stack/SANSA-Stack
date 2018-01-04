package net.sansa_stack.rdf.spark.qualityassessment.utils

import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.DatasetUtils._
import net.sansa_stack.rdf.spark.utils.StatsPrefixes._
import java.net.URL
import java.net.MalformedURLException
import java.net.HttpURLConnection
import java.io.IOException
import java.net.ProtocolException


/*
 * Node Utils.
 */
object NodeUtils {

  /**
   *  Checks if a resource ?node is local
   */
  def isInternal(node: Node) = prefixes.exists(prefix => (if (node.isLiteral) node.getLiteralLexicalForm else node.toString()).startsWith(prefix))

  /**
   *  Checks if a resource ?node is local
   */
  def isExternal(node: Node) = !isInternal(node)

  /**
   * Test whether the given literal lexical form is
   * a legal lexical form of this datatype.
   */
  def isLexicalFormCompatibleWithDatatype(node: Node) = node.getLiteralDatatype().isValid(node.getLiteralLexicalForm)

  /**
   * Checks if a resource @node is broken
   */
  def isBroken(node: Node): Boolean = {
    var isBroken = false
    var extUrl: URL = null

    try {
      extUrl = new URL(node.getURI()) //retrieving extUrl
    } catch {
      case e: MalformedURLException => (isBroken = true)
    }

    var urlConn: HttpURLConnection = null
    try {
      urlConn = extUrl.openConnection().asInstanceOf[HttpURLConnection]
    } catch {
      case ioe: IOException => (isBroken = true) //IO Exception
      case e: Exception     => (isBroken = true) //General Exception
    }
    try {
      urlConn.setRequestMethod("HEAD")
    } catch {
      case e: ProtocolException => (isBroken = true) //Protocol error
    }

    var responseCode = 0;

    try {
      urlConn.connect();
      responseCode = urlConn.getResponseCode();
    } catch {
      case e: IOException => (isBroken = true) //Not able to retrieve response code
    }

    if (responseCode >= 200 && responseCode < 400) {
      isBroken = false
    } else {
      isBroken = true //Bad response code
    }

    isBroken
  }

  def isHashUri(node: Node): Boolean = node.getURI().indexOf("#") > -1
  
  def isLabeled(node:Node) = (if (node.isLiteral) node.getLiteralLexicalForm else node.toString).contains(RDFS_LABEL)
  
  def isRDFSClass(node:Node) = (if (node.isLiteral) node.getLiteralLexicalForm else node.toString).contains(RDFS_CLASS)
  def isOWLClass(node:Node) = (if (node.isLiteral) node.getLiteralLexicalForm else node.toString).contains(OWL_CLASS)
}