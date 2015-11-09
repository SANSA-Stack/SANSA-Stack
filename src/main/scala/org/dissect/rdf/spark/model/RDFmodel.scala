package org.dissect.rdf.spark.model

import java.io.InputStream

/**
 * ******************************************************************************
 *                                                                              *
 *                                RDF model                                     *
 *                                                                              *
 * ******************************************************************************
 */

/**
 * Any RDF Object
 */
trait Object

/**
 * A resource RDF value
 */
trait Resource extends Object

/**
 * A literal RDF value
 */
trait Literal extends Resource {

  //string valye of the Literal
  def stringValue: String
}

/**
 * A RDF resource URI
 */
trait Node extends Resource {

  //URI for the resource
  def uri: java.net.URI
}

/**
 * A blank RDF resource.
 */
case class BlankNode(val bnID: String) extends Resource {

  override def toString() = "_" + bnID

}

/**
 * A unique URI name.
 * @param uri The URI value of the node.
 */
case class URI(val uri: java.net.URI) extends Node {

  override def toString() = "<" + uri + ">"

  override def equals(other: Any): Boolean =
    other match {
      case that: URI => (that isEqual (this)) && this.uri == that.uri
      case _ => false
    }

  final def isEqual(other: Any): Boolean = other.isInstanceOf[URI]
}

/**
 * A plain RDF literal
 * @param value The value of the literal
 * @param ltagOpt The optional language tag
 */
case class PlainLiteral(val value: String, ltagOpt: Option[Lang]) extends Literal {

  def stringValue = this.value

  def langExt = ltagOpt map { "@" + _.code } getOrElse ""

  override def toString() = "\"" + stringValue + "\""
}

/**
 * A RDF literal with a type
 * @param value The value of the literal
 * @param typ The type of the literal
 */
case class TypedLiteral(val value: String, val datatype: Node) extends Literal {

  def stringValue = this.value

  override def toString() = "\"" + stringValue + "\"^^" + datatype
}

/**
 * A language tag, used in PlainLiteral.
 */
sealed class Lang(c: String) {

  val code = c.toLowerCase()

  override def equals(a: Any): Boolean = a match {
    case that: Lang => this.code == that.code
    case _ => false
  }

  override lazy val hashCode = code.hashCode()
  override lazy val toString = code

}

/**
 * ******************************************************************************
 *                                                                              *
 *                                RDF Triples                                   *
 *                                                                              *
 * ******************************************************************************
 */

/**
 * An RDF triple.
 * @param subj The subject of the triple
 * @param pred The predicate (property) of the triple
 * @param obj The object of the triple
 */
class Triple(val subj: Node, val pred: URI, val obj: Node) {

  override def toString() = subj + " " + pred + " " + obj

  /*
   * isBankNode check if triple is declared as blank node or not?
   *
   */
  def isBlankNode() = this match {
    case Triple(b: Node, _, _) => true
    case Triple(_, _, b: Node) => true
    case _ => false
  }
  
  override def equals(other: Any) = {
    other match {
      case Triple(s, p, o) => subj == s && pred == p && obj == 0
      case _ => false
    }
  }

}
object Triple {
  def apply(s: Node, p: URI, o: Node) = new Triple(s, p, o)
  def unapply(statement: Triple): Option[Tuple3[Node, URI, Node]] = {
    Some(statement.subj, statement.pred, statement.obj)
  }
}

/**
 * ******************************************************************************
 *                                                                              *
 *                                new Model                                     *
 *                                                                              *
 * ******************************************************************************
 */

class newNode(val dt: String, val value: String, val lang: String) extends Serializable {
  override def toString: String = dt match {
    case "stringNode" => if (lang != null) "%s[%s]".format(value, lang) else value
    case "uriNode" => "<" + value + ">"
    case _=> value
  }
}

object newNode {
  def apply(item: String) = {
    item.trim match {
      case x if (x.startsWith("<") && x.endsWith(">")) => new newNode("uriNode", newRDF.startDelimeter(x), null)
      case x if (x.startsWith("\"") && x.endsWith("\"")) => new newNode("stringNode", newRDF.startDelimeter(x), null)
      case x if (x.startsWith("_:")) => new newNode("blankNode", x.substring(2), null)
      case x => new newNode("null", "null", "null")
    }
  }
}

object newRDF {

  def endDelimeter(line: String): String = {
    if (line.length() < 2 || line.takeRight(2) != " .") line else line.take(line.length() - 2)
  }

  def startDelimeter(item: String): String = if (item.length >= 2) item.substring(1, item.length - 1) else item

}

class newTriple(val subj: newNode, val pred: newNode, val obj: newNode) extends Serializable {
  override def toString = subj + " " + pred + " " + obj
}

object newTriple {
  def apply(nt: String) = {
    val parts= newRDF.endDelimeter(nt).split("\\s+", 3)
    new newTriple(newNode(parts(0)), newNode(parts(1)), newNode(parts(2)))
  }
}

class StringInputStream(s: String) extends InputStream {
  private val bytes = s.getBytes
  private var pos = 0

  override def read(): Int = if (pos >= bytes.length) {
    -1
  } else {
    val r = bytes(pos)
    pos += 1
    r.toInt
  }
}
