package org.dissect.rdf.spark.graph

import org.dissect.rdf.spark.utils._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object LoadGraph extends Logging {

  var _graph: Graph[String, String] = null
  //var _graph: Graph[VertexAtt, EdgeAtt] = null

  def apply(filePath: String, sparkContext: SparkContext) {
    val text = sparkContext.textFile(filePath) //hdfsfile + "/gsejdiu/DistLODStats/Dbpedia/en/geonames_links_en.nt.bz2")
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))

    val rs = text.map(NTriplesParser.parseTriple)

    val indexedmap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithIndex //indexing
    val vertices: RDD[(VertexId, String)] = indexedmap.map(x => (x._2, x._1))

    val tuples = rs.keyBy(_._1).join(indexedmap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[String]] = tuples.join(indexedmap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    _graph = Graph(vertices, edges)

  }

}