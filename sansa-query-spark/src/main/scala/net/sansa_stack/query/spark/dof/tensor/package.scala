package net.sansa_stack.query.spark.dof

import scala.collection.mutable.Map

package object tensor {
  type TensorModels = Map[Int, RDDTensor]
  val TensorModels = Map[Int, RDDTensor]()
}
