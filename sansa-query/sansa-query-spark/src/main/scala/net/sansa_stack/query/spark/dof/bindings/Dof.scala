package net.sansa_stack.query.spark.dof.bindings

import net.sansa_stack.query.spark.dof.node.{DofTriple, Helper}
import org.apache.jena.graph.{Node, Triple}

object Dof {

  def apply[R, N, T, A](bindings: Bindings[R, N, T, A], dofTriple: DofTriple): Unit =
    Case(bindings, dofTriple).process()

  def dof(node: Node): Int = if (node.isVariable) 1 else -1

  def dof(triple: Triple): Int = {
    var result = 0
    Helper.getNodes(triple).foreach(result += dof(_))
    result
  }
}
