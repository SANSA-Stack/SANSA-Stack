package net.sansa_stack.rdf.spark.utils

import java.io._

import org.apache.spark.graphx.Graph

object GraphUtils {

  def saveGraphToJson(graph: Graph[_, _], fn: String): Unit = {
    val pw = new PrintWriter(new File(fn))
    pw.println(toGraphJson(graph))
    pw.close
  }

  def toGraphJson(graph: Graph[_, _]): String = {
    val verts = graph.vertices.collect
    val edges = graph.edges.collect

    val nmap = verts.zipWithIndex.map(v => (v._1._1.toLong, v._2)).toMap

    val vtxt = verts.map(v =>
      "{\"val\":\"" + v._2.toString + "\"}").mkString(",\n")

    val etxt = edges.map(e =>
      "{\"source\":" + nmap(e.srcId).toString +
        ",\"target\":" + nmap(e.dstId).toString +
        ",\"value\":" + e.attr.toString + "}").mkString(",\n")

    "{ \"nodes\":[\n" + vtxt + "\n],\"links\":[" + etxt + "]\n}"
  }
}
