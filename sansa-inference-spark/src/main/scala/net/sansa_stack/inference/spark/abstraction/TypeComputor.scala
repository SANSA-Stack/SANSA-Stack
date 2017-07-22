package net.sansa_stack.inference.spark.abstraction

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.rdd.RDD
/**
  * @author Lorenz Buehmann
  */
trait TypeComputor {

  /**
    * Compute the types for all individuals, taking equivalent individuals into
    * account.
    * @param aboxTriples contains the instance data, i.e. the class assertions and property assertions
    */
  def computeTypes(aboxTriples: RDD[Triple]): RDD[((Set[Node], Set[Node], Set[Node]), Iterable[Node])]

}
