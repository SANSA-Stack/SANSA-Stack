package net.sansa_stack.query.spark.dof.sparql

import net.sansa_stack.query.spark.dof.bindings.Result
import net.sansa_stack.query.spark.dof.tensor.Tensor
import org.apache.jena.query.{ Query, QueryFactory }
import scala.reflect.ClassTag

object QueryExecutionFactory {

  private def checkNotNull(obj: Object, msg: String) = {
    if (obj == null) {
      throw new IllegalArgumentException(msg)
    }
  }

  private def checkArg[R, N: ClassTag, T, A](model: Tensor[R, N, T, A]) = checkNotNull(model, "Tensor is a null pointer")

  private def checkArg(queryStr: String) = checkNotNull(queryStr, "Query string is null")

  private def checkArg(query: Query) = checkNotNull(query, "Query is null")

  def makeQuery(queryStr: String): Query = QueryFactory.create(queryStr)

  def create[R, N: ClassTag, T, A](query: Query, tensor: Tensor[R, N, T, A]): Result[A] = {
    checkArg(query)
    checkArg(tensor)
    make(query, tensor)
  }

  def create[R, N: ClassTag, T, A](queryStr: String, tensor: Tensor[R, N, T, A]): Result[A] = {
    checkArg(queryStr)
    create(makeQuery(queryStr), tensor);
  }

  private def make[R, N: ClassTag, T, A](query: Query, model: Tensor[R, N, T, A]) = QueryExecution[R, N, T, A](query, model)
}
