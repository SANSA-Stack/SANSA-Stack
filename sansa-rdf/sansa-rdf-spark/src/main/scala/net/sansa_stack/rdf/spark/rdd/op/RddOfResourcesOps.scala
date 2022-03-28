package net.sansa_stack.rdf.spark.rdd.op

import org.apache.jena.rdf.model.impl.{LiteralImpl, ModelCom, ResourceImpl}
import org.apache.jena.rdf.model.{Model, RDFNode}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RddOfResourcesOps {
  def mapAs[T <: RDFNode](implicit t: ClassTag[T], rddOfRdfNodes: RDD[_ <: RDFNode], clazz: Class[T]): RDD[T] =
    rddOfRdfNodes.map(_.as(clazz))

  def mapToModels[T <: RDFNode](rddOfRdfNodes: RDD[_ <: RDFNode]): RDD[Model] =
    rddOfRdfNodes.map(_.getModel)

  /**
   * Enforce all Resources to be represented by ResourceImpl
   * This allows for reuse of default serializers for RDFNode ResourceImpl
   * and LiteralImpl
   *
   * @param rddOfRdfNodes
   * @return
   */
  def mapToDefaultRdfNodeImpls[T <: RDFNode](rddOfRdfNodes: RDD[_ <: RDFNode]): RDD[RDFNode] =
    rddOfRdfNodes.map(x =>
      if (x.isResource) {
        if (classOf[ResourceImpl].equals(x.getClass)) {
          x
        } else {
          new ResourceImpl(x.asNode(), x.getModel.asInstanceOf[ModelCom])
        }
      } else {
        if (classOf[LiteralImpl].equals(x.getClass)) {
          x
        } else {
          new LiteralImpl(x.asNode(), x.getModel.asInstanceOf[ModelCom])
        }
      })
}
