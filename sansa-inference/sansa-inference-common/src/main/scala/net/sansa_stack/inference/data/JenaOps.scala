package net.sansa_stack.inference.data

import org.apache.jena.datatypes.{BaseDatatype, RDFDatatype, TypeMapper}
import org.apache.jena.graph.{Graph => JenaGraph, Node => JenaNode, Triple => JenaTriple, _}
import org.apache.jena.rdf.model.{Seq => _}
import org.apache.jena.sparql.graph.GraphFactory

import scala.collection.JavaConverters._

class JenaOps extends RDFOps[Jena]  {

  // graph

  val emptyGraph: Jena#Graph = GraphFactory.createDefaultGraph

  def makeGraph(triples: Iterable[Jena#Triple]): Jena#Graph = {
    val graph: JenaGraph = GraphFactory.createDefaultGraph
    triples.foreach { triple =>
      graph.add(triple)
    }
    graph
  }

  def getTriples(graph: Jena#Graph): Iterable[Jena#Triple] =
    graph.find(JenaNode.ANY, JenaNode.ANY, JenaNode.ANY).asScala.to(Iterable)

  // triple

  def makeTriple(s: Jena#Node, p: Jena#URI, o: Jena#Node): Jena#Triple = {
    JenaTriple.create(s, p, o)
  }

  def fromTriple(t: Jena#Triple): (Jena#Node, Jena#URI, Jena#Node) = {
    val s = t.getSubject
    val p = t.getPredicate
    val o = t.getObject
    p match {
      case uri: Node_URI =>
        (s, uri, o)
      case _ =>
        throw new RuntimeException("fromTriple: predicate " + p.toString() + " must be a URI")
    }
  }

  // node

  def foldNode[T](node: Jena#Node)(funURI: Jena#URI => T, funBNode: Jena#BNode => T, funLiteral: Jena#Literal => T): T = node match {
    case iri: Jena#URI => funURI(iri)
    case bnode: Jena#BNode => funBNode(bnode)
    case literal: Jena#Literal => funLiteral(literal)
  }

  // URI

  def makeUri(iriStr: String): Jena#URI = { NodeFactory.createURI(iriStr).asInstanceOf[Node_URI] }

  def fromUri(node: Jena#URI): String =
    if (node.isURI) {
      node.getURI
    } else {
      throw new RuntimeException("fromUri: " + node.toString() + " must be a URI")
    }

  // bnode

  def makeBNode(): Node_Blank = NodeFactory.createBlankNode().asInstanceOf[Node_Blank]

  def makeBNodeLabel(label: String): Jena#BNode = {
    // val id = BlankNodeId.create(label)
    NodeFactory.createBlankNode(label).asInstanceOf[Node_Blank] // .createBlankNode(id)
  }

  def fromBNode(bn: Jena#BNode): String =
    if (bn.isBlank) {
      bn.getBlankNodeLabel
    } else {
      throw new RuntimeException("fromBNode: " + bn.toString + " must be a BNode")
    }

  // literal

  // TODO the javadoc doesn't say if this is thread safe
  lazy val mapper = TypeMapper.getInstance

  private def jenaDatatype(datatype: Jena#URI) = {
    val iriString = fromUri(datatype)
    val typ = mapper.getTypeByName(iriString)
    if (typ == null) {
      val datatype = new BaseDatatype(iriString)
      mapper.registerDatatype(datatype)
      datatype
    } else {
      typ
    }
  }

  val __xsdString: RDFDatatype = mapper.getTypeByName("http://www.w3.org/2001/XMLSchema#string")
  val __xsdStringURI: Jena#URI = makeUri("http://www.w3.org/2001/XMLSchema#string")
  val __rdfLangStringURI: Jena#URI = makeUri("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")

  def makeLiteral(lexicalForm: String, datatype: Jena#URI): Jena#Literal =
    if (datatype == __xsdStringURI) {
      NodeFactory.createLiteral(lexicalForm, null, null).asInstanceOf[Node_Literal]
    } else {
      NodeFactory.createLiteral(lexicalForm, null, jenaDatatype(datatype)).asInstanceOf[Node_Literal]
    }

  def makeLangTaggedLiteral(lexicalForm: String, lang: Jena#Lang): Jena#Literal =
    NodeFactory.createLiteral(lexicalForm, fromLang(lang), null).asInstanceOf[Node_Literal]


  // lang

  def makeLang(langString: String): String = langString

  def fromLang(lang: Jena#Lang): String = lang



}
