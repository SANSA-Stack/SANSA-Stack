package net.sansa_stack.rdf.common.partition.utils

import org.apache.jena.datatypes.TypeMapper
import org.apache.jena.graph.{ Node, NodeFactory }

/**
 * This class/object is not used (yet/anymore) but for now I keep it for reference ~ Claus 2016/11/17
 *
 */
case class RdfTerm(t: Int, v: String, lang: String, dt: String)

object RdfTerm {
  def toLexicalForm(o: Any): String = "" + o // NodeFmtLib.str(node)

  def termToNode(term: RdfTerm): Node = {
    val lexicalForm = toLexicalForm(term.v)
    val result: Node = term.t match {
      case 0 => NodeFactory.createBlankNode(lexicalForm)
      case 1 => NodeFactory.createURI(lexicalForm)
      case 2 =>
        val dt = term.dt
        if (term.dt != null && term.dt.nonEmpty) {

        }
        // {
        // logger.warn("Language tag should be null or empty, was '" + dt + "'");
        // }
        NodeFactory.createLiteral(lexicalForm, term.lang)
      case 3 => // Typed Literal
        val lang = term.lang
        // if (lang != null && !lang.isEmpty()) {
        // logger.warn("Language tag should be null or empty, was '" + lang + "'");
        // }
        val dt = TypeMapper.getInstance().getSafeTypeByName(term.dt)
        NodeFactory.createLiteral(lexicalForm, dt)
    }
    result
  }

  def nodeToTerm(node: Node): RdfTerm = {
    var t: Int = 0
    var v: Any = ""
    var lang: String = null
    var dt: String = null

    if (node.isBlank) {
      t = 0
      v = node.getBlankNodeLabel
    } else if (node.isURI) {
      t = 1
      v = node.getURI
    } else if (node.isLiteral) {

      v = node.getLiteral.getValue

      // lex = node.getLiteralLexicalForm();

      dt = node.getLiteralDatatypeURI
      if (dt == null || dt.isEmpty) {
        // System.err.println("Treating plain literals as typed ones");
        // logger.warn("Treating plain literals as typed ones");
        t = 2
        lang = node.getLiteralLanguage
      } else {
        t = 3
        dt = node.getLiteralDatatypeURI
      }
    } else {
      throw new RuntimeException("Should not happen")
    }

    var dtStr = if (dt == null) "" else dt
    var langStr = if (lang == null) "" else lang

    RdfTerm(t, "" + v, lang, dt)
  }
}
