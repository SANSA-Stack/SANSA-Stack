package net.sansa_stack.rdf.spark.graph

import scala.collection.immutable.Map

class VertexAtt(val attr: String) extends Serializable {

  val parentNodes: Map[Long, String] = null;

  val childrenNodes: Map[Long, String] = null;

}