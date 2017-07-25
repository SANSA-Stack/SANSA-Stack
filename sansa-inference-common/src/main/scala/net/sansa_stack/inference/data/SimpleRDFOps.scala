package net.sansa_stack.inference.data

import org.apache.jena.graph.NodeFactory

/**
  * @author Lorenz Buehmann
  */
class SimpleRDFOps extends RDFOps[SimpleRDF]  {

    // triple

    def makeTriple(s: SimpleRDF#Node, p: SimpleRDF#URI, o: SimpleRDF#Node): SimpleRDF#Triple = {
        RDFTriple(s, p, o)
    }

    def fromTriple(t: SimpleRDF#Triple): (SimpleRDF#Node, SimpleRDF#URI, SimpleRDF#Node) = {
        val s = t.s
        val p = t.p
        val o = t.o
        if (p.isInstanceOf[SimpleRDF#URI])
            (s, p.asInstanceOf[SimpleRDF#URI], o)
        else
            throw new RuntimeException("fromTriple: predicate " + p.toString + " must be a URI")
    }

    // node

    def foldNode[T](node: SimpleRDF#Node)(funURI: SimpleRDF#URI => T, funBNode: SimpleRDF#BNode => T, funLiteral: SimpleRDF#Literal => T): T = node match {
        case iri: SimpleRDF#URI => funURI(iri)
        case bnode: SimpleRDF#BNode => funBNode(bnode)
        case literal: SimpleRDF#Literal => funLiteral(literal)
    }

    // URI

    def makeUri(iriStr: String): SimpleRDF#URI = { iriStr }

    def fromUri(node: SimpleRDF#URI): String = node.asInstanceOf[String]

    // literal


    def makeLiteral(lexicalForm: String, datatype: SimpleRDF#URI): SimpleRDF#Literal = "\"" + lexicalForm + "\"^^" + datatype

    def makeLangTaggedLiteral(lexicalForm: String, lang: SimpleRDF#Lang): SimpleRDF#Literal = "\"" + lexicalForm + "\"@" + lang


    // lang

    def makeLang(langString: String): SimpleRDF#Lang = langString

    def fromLang(lang: SimpleRDF#Lang): String = lang

    override def makeBNode(): String = NodeFactory.createBlankNode().toString

    override def makeBNodeLabel(s: String): String = s

    override def fromBNode(bn: String): String = bn

}
