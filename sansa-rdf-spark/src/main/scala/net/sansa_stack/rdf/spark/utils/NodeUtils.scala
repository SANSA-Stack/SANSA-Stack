package net.sansa_stack.rdf.spark.utils

import org.apache.jena.graph.{ Node, Triple }

object NodeUtils {

  /**
   * Return node value based on its type
   * @param node the Node to be check
   * @return node value (case when node is URI:: URI, when node is Blank ::Its blank node ID, when node is literal:: its Literal).
   */
  def getNodeValue(node: Node): String = node match {
    case uri if node.isURI => node.getURI
    case blank if node.isBlank => node.getBlankNodeId.toString
    case literal if node.isLiteral => node.getLiteral.toString
    case _ => throw new IllegalArgumentException(s"${node.getLiteralLexicalForm} is not valid!")
  }

}
