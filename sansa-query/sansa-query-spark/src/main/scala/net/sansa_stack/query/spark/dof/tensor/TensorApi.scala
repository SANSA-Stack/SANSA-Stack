package net.sansa_stack.query.spark.dof.tensor

import net.sansa_stack.query.spark.dof.bindings.Result
import net.sansa_stack.query.spark.dof.node.{ SPO, VariableMap }
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.sparql.core.Var
import org.apache.spark.rdd.RDD

trait TensorApi[R, N, T, A] extends java.io.Serializable {

  def readData: R
  def buildTensor: T
  def buildSPO: SPO[Node]

  def process(triple: Triple, mapV: VariableMap[N]): A

  def saveResult(triple: Triple, result: Result[A], current: A): Result[A]

  def unionResult(result: Result[A], current: Result[A]): Result[A]

  def getEmptyRDD: RDD[N]

  def output(result: Result[A], resultVars: List[Var]): Array[String]

  def compareResult(result: Result[A], referenceResult: RDD[String], resultVars: List[Var]): Boolean
}
