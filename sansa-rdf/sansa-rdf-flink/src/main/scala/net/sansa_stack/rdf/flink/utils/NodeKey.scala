package net.sansa_stack.rdf.flink.utils

import org.apache.jena.graph.Node
import org.apache.jena.sparql.util.NodeComparator

/**
  * Key type wrapper for Jena `Node` objects.
  * It basically makes Node comparable which is necessary to be handles as key in Flink.
  *
  * @author Lorenz Buehmann
  */
class NodeKey(val node: Node) extends Comparable[NodeKey] with Equals {

  override def compareTo(o: NodeKey): Int = {
    val other = o.node
    if (node == null)
      if (other == null) 0 else -1
    else if (other == null) 1 else new NodeComparator().compare(node, other)
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[NodeKey]

  override def hashCode(): Int = 31 * node.##

  override def equals(that: Any): Boolean =
    that match {
      case key: NodeKey => (this eq key) || (key.canEqual(this) && hashCode == key.hashCode)
      case _ => false
    }
}

object NodeKey {
  def apply(node: Node): NodeKey = new NodeKey(node)
}
