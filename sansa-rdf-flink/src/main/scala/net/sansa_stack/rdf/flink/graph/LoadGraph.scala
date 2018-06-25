package net.sansa_stack.rdf.flink.graph

import scala.collection.JavaConverters._

import net.sansa_stack.rdf.flink.data.RDFGraph
import net.sansa_stack.rdf.flink.model.RDFTriple
import net.sansa_stack.rdf.flink.utils.Logging
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.graph.{ Edge, Vertex }
import org.apache.flink.graph.scala._
import org.apache.flink.types.NullValue
import org.apache.jena.graph.Node

object LoadGraph extends Logging {

  def apply(rdfgraph: RDFGraph, env: ExecutionEnvironment): Graph[Long, Node, Node] = {

    val triples = rdfgraph.triples

    val vertexIDs: DataSet[(Node, Long)] = (triples.map(_.subject) union triples.map(_.`object`)).distinct.map(f => (f, f.getURI.toLong)) // indexing

    val vertices: DataSet[(Long, Node)] = vertexIDs.map(x => (x._2, x._1))

    val spo: DataSet[(Node, (Node, Node))] = triples.map {
      _ match {
        case RDFTriple(s, p, o) => (s, (p, o))
      }
    }

    val tuples = spo.join(vertexIDs).where(0).equalTo(1).map {
      _ match {
        case ((s, (p, o)), (sv, sid)) => (o, (sid, p))
      }
    }

    val edges = tuples.join(vertexIDs).where(0).equalTo(1).map {
      _ match {
        case ((k, (si, p)), (sv, oi)) => new Edge(si, oi, p)
      }
    }

    val v = vertices.map(f => new Vertex(f._1, f._2))

    Graph.fromDataSet(vertices.map(f => new Vertex(f._1, f._2)), edges, env)
  }

}
