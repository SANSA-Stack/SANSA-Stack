package net.sansa_stack.query.spark.dof.bindings

import org.apache.jena.sparql.core.Var
import scala.reflect.ClassTag

/*
 * Resulting class for bindings
 * */
class Result[A](_rdd: A, _keys: List[Var]) {
  def rdd: A = _rdd
  def keys: List[Var] = _keys
}
