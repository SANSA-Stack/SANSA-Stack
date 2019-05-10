package net.sansa_stack.query.spark.dof.sparql

import net.sansa_stack.query.spark.dof.bindings.{ Bindings, Dof, Result }
import net.sansa_stack.query.spark.dof.node.{ Constraints, Helper }
import net.sansa_stack.query.spark.dof.tensor.Tensor
import scala.reflect.ClassTag

class Builder[R, N: ClassTag, T, A](model: Tensor[R, N, T, A], constraints: Constraints) {

  def application: Result[A] = {
    var proceed = true
    val bindings = new Bindings(model, constraints)
    var T = bindings.recalcDof(constraints.dofs)
    while (proceed && T.nonEmpty) {
      val triple = T(0)
      T = T.drop(1)
      Dof(bindings, triple)
      T = bindings.recalcDof(T)
    }
    bindings.result
  }
}
