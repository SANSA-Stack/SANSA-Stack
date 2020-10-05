package net.sansa_stack.query.spark.dof.sparql

import net.sansa_stack.query.spark.dof.bindings.Result
import net.sansa_stack.query.spark.dof.tensor.Tensor
import org.apache.jena.query.Query
import scala.reflect.ClassTag

object QueryExecution {
  def apply[R, N: ClassTag, T, A](query: Query, model: Tensor[R, N, T, A]):
    Result[A] = QueryWalker.walk(query, model)
}
