package net.sansa_stack.query.spark.dof.node

import java.io.{ File, PrintWriter }

import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD

object Helper {
  var pw: PrintWriter = _
  def init(outputDir: String): Unit = {
    val file = new File((if (outputDir == null || outputDir.isEmpty) "." else outputDir) + "/log.dat")
    // if (file.exists()) file.delete()
    pw = new PrintWriter(file)
  }

  var DEBUG = false

  var SHOW_TIME_MEASUREMENT = false

  def start: Long = System.nanoTime

  def duration(start: Long): Long = ((System.nanoTime - start) / 1e6d).round
  // BigDecimal(((System.nanoTime - start) / 1e6d)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

  def measure(start: Long, msg: String): String = msg + " (ms): " + duration(start) // + "s."

  def measureTime(start: Long, msg: String): Unit = if (SHOW_TIME_MEASUREMENT) {
    log(measure(start, msg))
  }

  def log(s: String): Unit = if (DEBUG) { println(s); pw.write(s + "\n") }

  def log(o: Object): Unit = log(o.toString())

  def close: Unit = pw.close

  // USe predicates first for a kind o vertical partitioning
  // since number of predicates is usually less than number of subjects and objects
  val nodeMethods = Array("getPredicate", "getSubject", "getObject")
  val nodeMethodsZip = nodeMethods.zipWithIndex

  def getNodeMethods: Array[String] = nodeMethods

  def getNodes(triple: Triple): Array[Node] = Array(triple.getPredicate, triple.getSubject, triple.getObject)

  def getNode(triple: Triple, method: String): Node = triple.getClass.getMethod(method).invoke(triple).asInstanceOf[Node]

  def getSPOColumn[N](spo: SPO[N], method: String): NodeIndexed[N] =
    spo.getClass.getMethod(method).invoke(spo).asInstanceOf[NodeIndexed[N]]

  def getNodeIndex[N](spo: SPO[N], method: String, nodeToFind: N): RDD[Long] =
    getSPOColumn(spo, method).getIndex(nodeToFind)

  def getNodeIndex[N](spoColumn: NodeIndexed[N], method: String, nodeToFind: N): RDD[Long] =
    spoColumn.getIndex(nodeToFind)

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
