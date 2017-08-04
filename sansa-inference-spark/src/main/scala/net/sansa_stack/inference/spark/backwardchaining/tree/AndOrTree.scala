package net.sansa_stack.inference.spark.backwardchaining.tree

import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule

/**
  * @author Lorenz Buehmann
  */
class AndOrTree(val root: AndNode) {
  def depth: Unit = {}

  def size: Unit = {}

  override def toString: String = root.print(0)
}

abstract class Node[T, C <: Node[_, _]](val element: T, var children: Seq[C] = Seq()) {
  override def toString: String = print(0)

  def print(indent: Int): String = {
    renderElement() + "\n" + children.map(c => "---" * indent + c.print(indent + 1)).mkString("\n")
  }

  def renderElement(): String = element.toString
}

class OrNode(override val element: Rule) extends Node[Rule, AndNode](element) {
  override def renderElement(): String = element.getName
}

class AndNode(override val element: TriplePattern) extends Node[TriplePattern, OrNode](element) {}

object AndOrTree {
  def apply(root: AndNode): AndOrTree = new AndOrTree(root)
}
