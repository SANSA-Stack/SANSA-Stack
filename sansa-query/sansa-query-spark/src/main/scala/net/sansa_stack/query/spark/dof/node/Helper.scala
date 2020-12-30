package net.sansa_stack.query.spark.dof.node

import org.apache.jena.graph.{Node, Triple}

object Helper {
  val nodeMethods = Array("getPredicate", "getSubject", "getObject")
  val nodeMethodsZip = nodeMethods.zipWithIndex

  def getNodeMethods: Array[String] = nodeMethods

  def getNodes(triple: Triple): Array[Node] = Array(triple.getPredicate, triple.getSubject, triple.getObject)

  def getNode(triple: Triple, method: String): Node = triple.getClass.getMethod(method).invoke(triple).asInstanceOf[Node]

  def getNodeByMethod[N](method: String, s: N, p: N, o: N): N = {
    if (method.equals(nodeMethods(0))) {
      return p
    }
    if (method.equals(nodeMethods(1))) {
      return s
    }
    if (method.equals(nodeMethods(2))) {
      return o
    }
    throw new IllegalArgumentException("Invalid method name in tuple: (" + s + " " + p + " " + o + ")")
  }
}
