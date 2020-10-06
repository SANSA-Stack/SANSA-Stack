package net.sansa_stack.query.spark.dof.node

import net.sansa_stack.query.spark.dof.bindings.Dof
import org.apache.jena.sparql.core.{ TriplePath, Var }

class Constraints(_vars: Vars, _dofs: DofTripleList) {
  def vars: Vars = _vars
  def dofs: DofTripleList = _dofs
}
