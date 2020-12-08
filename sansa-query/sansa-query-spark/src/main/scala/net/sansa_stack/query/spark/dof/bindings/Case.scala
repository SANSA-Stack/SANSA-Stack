package net.sansa_stack.query.spark.dof.bindings

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.core.Var

import net.sansa_stack.query.spark.dof.node.{DofTriple, Helper}

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

  def getEmptyRowVarMap(triple: Triple): Map[Var, Node] = getRowVarMap(triple, Node.NULL, Node.NULL, Node.NULL)

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
