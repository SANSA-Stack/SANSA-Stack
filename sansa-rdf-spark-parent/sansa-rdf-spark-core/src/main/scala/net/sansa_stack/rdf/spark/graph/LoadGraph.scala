package net.sansa_stack.rdf.spark.graph

import net.sansa_stack.rdf.spark.utils._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import net.sansa_stack.rdf.spark.model.JenaSparkRDDOps

object LoadGraph extends Logging {

  def apply(filePath: String, sparkContext: SparkContext) = {
    /*  val text = sparkContext.textFile(filePath) //hdfsfile + "/gsejdiu/DistLODStats/Dbpedia/en/geonames_links_en.nt.bz2")
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
*/
    val ops = JenaSparkRDDOps(sparkContext)
    import ops._

    val text = fromNTriples(filePath, "http://dbpedia.org")
    val rs = ops.sparkContext.parallelize((text.map(x =>
      (x.getSubject.getLiteralLexicalForm, x.getPredicate.getLiteralLexicalForm, x.getObject.getLiteralLexicalForm)).toList))

    val indexedmap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithIndex //indexing

    val vertices: RDD[(VertexId, String)] = indexedmap.map(x => (x._2, x._1))
    val _iriToId: RDD[(String, VertexId)] = indexedmap.map(x => (x._1, x._2))

    val tuples = rs.keyBy(_._1).join(indexedmap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[String]] = tuples.join(indexedmap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    Graph(null, null)//TODO

    new {
      val graph = Graph(null, null)
      val iriToId = _iriToId
    }

  }

}