package net.sansa_stack.query.spark.dof.bindings

import net.sansa_stack.query.spark.dof.node.{DofTriple, Helper}
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.core.Var

object Case{
  def apply[R, N, T, A]
  (bindings: Bindings[R, N, T, A], dofTriple: DofTriple)
    : Case[R, N, T, A] = {
    val dof = dofTriple._1
    dof match {
      case -3 | -1 | 1 | 3 => new Case(bindings, dofTriple)
      case _ => throw new Exception("Illegal value for dof=" + dof)
    }
  }

  // I replaced the removed-due-to-deprecation Node.NULL with Node.ANY;
  // as this seems to be pattern matching code it may be appropriate ~ Claus 2021-06-29
  def getEmptyRowVarMap(triple: Triple): Map[Var, Node] = getRowVarMap(triple, Node.ANY, Node.ANY, Node.ANY)

  def getRowVarMap[N](triple: Triple, s: N, p: N, o: N): Map[Var, N] = {
    var map = Map[Var, N]()
    Helper.getNodeMethods.foreach(method => {
      val node = Helper.getNode(triple, method)
      node match {
        case nv: Var =>
          map = map + (nv -> Helper.getNodeByMethod(method, s, p, o))
        case _ =>
      }
    })
    map
  }
}

class Case[R, N, T, A] (bindings: Bindings[R, N, T, A], dofTriple: DofTriple)
      extends Serializable {
  def process(): Unit = {
    val triple = dofTriple._2
    val result = bindings.getModel.process(triple, bindings.mapV)
    bindings.saveResult(triple, result)
  }
}
