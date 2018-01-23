package net.sansa_stack.rdf.spark.graph

import net.sansa_stack.rdf.spark.utils._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import net.sansa_stack.rdf.spark.model.JenaSparkRDDOps
import net.sansa_stack.rdf.spark.model.TripleRDD._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.sql.SparkSession

object LoadGraph extends Logging {

  @transient private var sparkSession: SparkSession = _

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

    //Graph(null, null) //TODO
    org.apache.spark.graphx.Graph(vertices, edges)

    /*new {
      val graph = org.apache.spark.graphx.Graph(vertices, edges)
      val iriToId = _iriToId
    }*/

  }

  def getNodeValue(node: Node): String = node match {
    case uri if node.isURI         => node.getURI
    case blank if node.isBlank     => node.getBlankNodeId.toString
    case literal if node.isLiteral => node.getLiteral.toString
    case _                         => throw new IllegalArgumentException(s"${node.getLiteralLexicalForm} is not valid!")
  }

  /**
   * Constructs GraphX graph from RDD of triples
   * @param triples rdd of triples
   * @return object of LoadGraph which contains the constructed  ''graph''.
   */
  def apply(triples: RDD[Triple]): Graph[Node, Node] = {
    val rs = triples.map(triple => (triple.getSubject, triple.getPredicate, triple.getObject))
    val indexedMap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithUniqueId()

    val vertices: RDD[(VertexId, Node)] = indexedMap.map(x => (x._2, x._1))
    val _nodeToId: RDD[(Node, VertexId)] = indexedMap.map(x => (x._1, x._2))

    val tuples = rs.keyBy(_._1).join(indexedMap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[Node]] = tuples.join(indexedMap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    org.apache.spark.graphx.Graph(vertices, edges)
  }

  /**
   * Constructs GraphX graph from RDD of triples
   * @param triples rdd of triples
   * @return object of LoadGraph which contains the constructed  ''graph''.
   */
  def asString(triples: RDD[Triple]): Graph[String, String] = {
    val rs = triples.map(triple => (getNodeValue(triple.getSubject), getNodeValue(triple.getPredicate), getNodeValue(triple.getObject)))
    val indexedMap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithUniqueId()

    val vertices: RDD[(VertexId, String)] = indexedMap.map(x => (x._2, x._1))
    val _nodeToId: RDD[(String, VertexId)] = indexedMap.map(x => (x._1, x._2))

    val tuples = rs.keyBy(_._1).join(indexedMap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[String]] = tuples.join(indexedMap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    org.apache.spark.graphx.Graph(vertices, edges)
  }

}
