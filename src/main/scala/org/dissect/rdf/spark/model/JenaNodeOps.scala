package org.dissect.rdf.spark.model

import org.apache.jena.datatypes.{RDFDatatype, TypeMapper}
import org.apache.jena.graph.{Node => JenaNode, Triple => JenaTriple, _}

/**
 * Jena based implementation of RDFNodeOps
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
trait JenaNodeOps[Rdf <: Jena]
  extends RDFNodeOps[Rdf]
  with DefaultURIOps[Rdf] {
  // triple

  protected def makeTriple(s: Rdf#Node, p: Rdf#URI, o: Rdf#Node): Rdf#Triple =
    JenaTriple.create(s, p, o)

  protected def fromTriple(t: Rdf#Triple): (Rdf#Node, Rdf#URI, Rdf#Node) = {
    val s = t.getSubject
    val p = t.getPredicate
    val o = t.getObject
    p match {
      case uri: Rdf#URI => (s, uri, o)
      case _ => throw new RuntimeException("fromTriple: predicate " + p.toString + " must be a URI")
    }
  }

  // node

  protected def foldNode[T](node: Rdf#Node)(funURI: Rdf#URI => T, funBNode: Rdf#BNode => T, funLiteral: Rdf#Literal => T): T = node match {
    case iri: Rdf#URI => funURI(iri)
    case bnode: Rdf#BNode => funBNode(bnode)
    case literal: Rdf#Literal => funLiteral(literal)
  }

  // URI

  protected def makeUri(iriStr: String): Rdf#URI =
    NodeFactory.createURI(iriStr).asInstanceOf[Node_URI]

  protected def fromUri(node: Rdf#URI): String =
    if (node.isURI)
      node.getURI
    else
      throw new RuntimeException("fromUri: " + node.toString + " must be a URI")

  // bnode

  protected def makeBNode(): Rdf#BNode =
    NodeFactory.createBlankNode().asInstanceOf[Node_Blank]

  protected def makeBNodeLabel(label: String): Rdf#BNode =
    NodeFactory.createBlankNode(label).asInstanceOf[Node_Blank]

  protected def fromBNode(bn: Rdf#BNode): String =
    if (bn.isBlank)
      bn.getBlankNodeLabel
    else
      throw new RuntimeException("fromBNode: " + bn.toString + " must be a BNode")

  // literal

  // ThreadLocal wrapper because TypeMapper's javadoc doesn't say if it is thread safe
  private val mapper: ThreadLocal[TypeMapper] = new ThreadLocal[TypeMapper] {
    override def initialValue = TypeMapper.getInstance()
  }

  protected def jenaDatatype(datatype: Rdf#URI) = {
    val iriString = fromUri(datatype)
    mapper.get.getSafeTypeByName(iriString)
  }

  protected val __xsdString: RDFDatatype = mapper.get.getTypeByName("http://www.w3.org/2001/XMLSchema#string")
  protected val __xsdStringURI: Rdf#URI = makeUri("http://www.w3.org/2001/XMLSchema#string")
  protected val __rdfLangStringURI: Rdf#URI = makeUri("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")

  protected def makeLiteral(lexicalForm: String, datatype: Rdf#URI): Rdf#Literal =
    if (datatype == __xsdStringURI)
      NodeFactory.createLiteral(lexicalForm, null, null).asInstanceOf[Node_Literal]
    else
      NodeFactory.createLiteral(lexicalForm, null, jenaDatatype(datatype)).asInstanceOf[Node_Literal]

  protected def makeLangTaggedLiteral(lexicalForm: String, lang: Rdf#Lang): Rdf#Literal =
    NodeFactory.createLiteral(lexicalForm, fromLang(lang), null).asInstanceOf[Node_Literal]

  protected def fromLiteral(literal: Rdf#Literal): (String, Rdf#URI, Option[Rdf#Lang]) = {
    val lexicalForm = literal.getLiteralLexicalForm
    val literalLanguage = literal.getLiteralLanguage
    def getDatatype: Rdf#URI = {
      val typ = literal.getLiteralDatatype
      if (typ == null) __xsdStringURI else makeUri(typ.getURI)
    }
    if (literalLanguage == null || literalLanguage.isEmpty)
      (lexicalForm, getDatatype, None)
    else
      (lexicalForm, __rdfLangStringURI, Some(makeLang(literalLanguage)))
  }

  // lang

  protected def makeLang(langString: String) = langString

  protected def fromLang(lang: Rdf#Lang) = lang

  // node matching

  protected val ANY: Rdf#NodeAny = JenaNode.ANY.asInstanceOf[Node_ANY]

  protected implicit def toConcreteNodeMatch(node: Rdf#Node): Rdf#NodeMatch = node.asInstanceOf[Rdf#Node]

  protected def foldNodeMatch[T](nodeMatch: Rdf#NodeMatch)(funANY: => T, funConcrete: Rdf#Node => T): T =
    if (nodeMatch == ANY)
      funANY
    else
      funConcrete(nodeMatch.asInstanceOf[JenaNode])

  protected def matches(node1: Rdf#Node, node2: Rdf#Node): Boolean =
    node1.matches(node2)
}
