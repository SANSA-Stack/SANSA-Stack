package net.sansa_stack.inference.data

/**
  * @author Lorenz Buehmann
  */
trait RDFOps[Rdf <: RDF] {

  // Triple
  def makeTriple(s: Rdf#Node, p: Rdf#URI, o: Rdf#Node): Rdf#Triple
  def fromTriple(triple: Rdf#Triple): (Rdf#Node, Rdf#URI, Rdf#Node)

  // URI
  def makeUri(s: String): Rdf#URI
  def fromUri(uri: Rdf#URI): String

  // blank node
  def makeBNode(): Rdf#BNode
  def makeBNodeLabel(s: String): Rdf#BNode
  def fromBNode(bn: Rdf#BNode): String

  // literal
  def makeLiteral(lexicalForm: String, datatype: Rdf#URI): Rdf#Literal
  def makeLangTaggedLiteral(lexicalForm: String, lang: Rdf#Lang): Rdf#Literal
//  def fromLiteral(literal: Rdf#Literal): (String, Rdf#URI, Option[Rdf#Lang])

  // lang
  def makeLang(s: String): Rdf#Lang
  def fromLang(l: Rdf#Lang): String
}

object RDFOps {
  def apply[Rdf <: RDF](implicit ops: RDFOps[Rdf]): RDFOps[Rdf] = ops
}
