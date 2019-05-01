package net.sansa_stack.query.spark.dof.bindings

import net.sansa_stack.query.spark.dof.node._
import net.sansa_stack.query.spark.dof.tensor.Tensor
import org.apache.jena.graph.Triple
import scala.reflect.ClassTag

/*
 * Class used to bind variables with their values
 */
class Bindings[R, N: ClassTag, T, A](model: Tensor[R, N, T, A], constraints: Constraints) extends Serializable {
  private var _mapV = new VariableMap[N](model.sparkContext, constraints)
  def mapV: VariableMap[N] = _mapV

  private var _result: Result[A] = _
  def result: Result[A] = _result
  def result_=(value: Result[A]): Unit = _result = value

  def getModel: Tensor[R, N, T, A] = model

  def saveResult(triple: Triple, rdd: A): Unit = {
    val start = Helper.start
    result = model.saveResult(triple, result, rdd)
    Helper.measureTime(start, s"\nSave result time=")
  }

  def recalcDof(triples: DofTripleList): List[(Int, Triple)] = {
    def calcDof(triple: Triple) = {
      // how many constants (or variables to which there exists a non-empty set associated in V)
      var k = 0;
      // how many variables (to which an empty set is associated in V)
      var v = 0;

      Helper.getNodes(triple).foreach(node => {
        if (mapV.isUnbounded(node)) {
          v += 1
        }
        else {
          k += 1
        }
      })

      // Helper.log("dof(t,V)=" + (v-k))
      v - k
    }

    triples.map(triple => calcDof(triple._2) -> triple._2).sortBy(_._1)
  }
}
