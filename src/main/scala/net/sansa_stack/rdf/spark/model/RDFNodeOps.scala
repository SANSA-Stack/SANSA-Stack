package net.sansa_stack.rdf.spark.model

/**
 * Operations on RDF nodes/literals/triples etc., ie. elements of a graph
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
trait RDFNodeOps[Rdf <: RDF]
  extends URIOps[Rdf] {
  // triple

  def fromNTriples(rdf: String, baseIRI: String): Iterable[Rdf#Triple]

  def toNTriples(triples: Iterable[Rdf#Triple]): String

  def makeTriple(s: Rdf#Node, p: Rdf#URI, o: Rdf#Node): Rdf#Triple

  def fromTriple(triple: Rdf#Triple): (Rdf#Node, Rdf#URI, Rdf#Node)

  // node

  def foldNode[T](node: Rdf#Node)(funURI: Rdf#URI => T, funBNode: Rdf#BNode => T, funLiteral: Rdf#Literal => T): T

  // URI

  def makeUri(s: String): Rdf#URI

  def fromUri(uri: Rdf#URI): String

  // bnode

  def makeBNode(): Rdf#BNode

  def makeBNodeLabel(s: String): Rdf#BNode

  def fromBNode(bn: Rdf#BNode): String

  // literal

  def makeLiteral(lexicalForm: String, datatype: Rdf#URI): Rdf#Literal

  def makeLangTaggedLiteral(lexicalForm: String, lang: Rdf#Lang): Rdf#Literal

  def fromLiteral(literal: Rdf#Literal): (String, Rdf#URI, Option[Rdf#Lang])

  // lang

  def makeLang(s: String): Rdf#Lang

  def fromLang(l: Rdf#Lang): String

  // node matching

  def ANY: Rdf#NodeAny

  implicit def toConcreteNodeMatch(node: Rdf#Node): Rdf#NodeMatch

  def foldNodeMatch[T](nodeMatch: Rdf#NodeMatch)(funANY: => T, funNode: Rdf#Node => T): T

  def matches(node1: Rdf#Node, node2: Rdf#Node): Boolean

  final def matchNode[T <: Rdf](node: T#Node, nodeMatch: T#NodeMatch): Boolean =
    foldNodeMatch(nodeMatch)(true, n => matches(node, n))
}

object RDFNodeOps {
  def apply[Rdf <: RDF](implicit ops: RDFNodeOps[Rdf]): RDFNodeOps[Rdf] = ops
}