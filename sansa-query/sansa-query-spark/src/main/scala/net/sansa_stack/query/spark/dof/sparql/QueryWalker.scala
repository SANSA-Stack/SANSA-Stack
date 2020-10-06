package net.sansa_stack.query.spark.dof.sparql

import net.sansa_stack.query.spark.dof.bindings.Result
import net.sansa_stack.query.spark.dof.tensor.Tensor
import org.apache.jena.query.Query
import org.apache.jena.sparql.syntax.ElementWalker
import scala.reflect.ClassTag

object QueryWalker {
  def walk[R, N: ClassTag, T, A](query: Query, model: Tensor[R, N, T, A]):
  Result[A] = {
    val visitor = new RecursiveElementVisitor(model)
    ElementWalker.walk(query.getQueryPattern(), visitor)
    visitor.result
  }
}
