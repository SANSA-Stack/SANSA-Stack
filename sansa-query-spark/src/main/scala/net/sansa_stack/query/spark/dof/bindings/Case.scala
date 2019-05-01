package net.sansa_stack.query.spark.dof.bindings

import net.sansa_stack.query.spark.dof.node.{ DofTriple, Helper }
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.query.Query
import org.apache.jena.sparql.core.Var
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParArray

object Case{
  // TODO: remove bindings, only query left?
  def apply[R, N, T, A]
  (bindings: Bindings[R, N, T, A], dofTriple: DofTriple)
    : Case[R, N, T, A] = {
    val dof = dofTriple._1
    Helper.log("\nCase " + dof + ": " + dofTriple._2)
    dof match {
      case -3 | -1 | 1 | 3 => return new Case(bindings, dofTriple)
      case _ => throw new Exception("Illegal value for dof=" + dof)
    }
  }

  def getEmptyRowVarMap(triple: Triple): Map[Var, Node] = getRowVarMap(triple, Node.NULL, Node.NULL, Node.NULL)

  def getRowVarMap[N](triple: Triple, s: N, p: N, o: N): Map[Var, N] = {
    var map = Map[Var, N]()
    Helper.getNodeMethods.foreach(method => {
      val node = Helper.getNode(triple, method)
      if (node.isInstanceOf[Var]) {
        val nv = node.asInstanceOf[Var]
        map = map + (nv -> Helper.getNodeByMethod(method, s, p, o))
      }
    })
    map
  }
}

class Case[R, N, T, A]
  (bindings: Bindings[R, N, T, A], dofTriple: DofTriple)
  extends Serializable {
  val model = bindings.getModel
  val triple = dofTriple._2
  val dof = dofTriple._1

  def process: Boolean = {
    val start = Helper.start
    val result = model.process(triple, bindings.mapV)
    Helper.measureTime(start, s"\nCase " + dof + " time=")
    bindings.saveResult(triple, result)
    true// TODO: check with false when a triple returns an empty result
  }
}
