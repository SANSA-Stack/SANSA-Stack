package org.dissect.rdf.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import java.io.File
import org.apache.commons.io.FileUtils
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import org.apache.jena.riot.RiotReader
import org.apache.jena.riot.Lang
import org.dissect.rdf.spark.utils._
import org.dissect.rdf.spark.model._
import org.dissect.rdf.spark.analitycs._
import org.dissect.rdf.spark.utils.Logging

object App extends Logging {

  val SPARK_MASTER = SparkUtils.SPARK_MASTER

  val componentLists = HashMap[VertexId, ListBuffer[VertexId]]()
  val prefLabelMap = HashMap[VertexId, String]()

  val prefLabelMapFiltered = HashMap[VertexId, ListBuffer[VertexId]]()

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: FileName <path-to-files> <output-path>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("BDE-readRDF").setMaster(SPARK_MASTER);
    val sparkContext = new SparkContext(sparkConf)

    val fileName: String = args(0)
    val rdfFile = new File(args(1) + fileName + ".nt") //, "UTF-8")
    //var rdfFile = new File(args(1),fileName + ".nt").getAbsolutePath()
    val outputPath = new File(args(2), fileName).getAbsolutePath();
    FileUtils.deleteDirectory(new File(outputPath))

    var literalPropsTriplesArray = new Array[(Long, Long, String)](0)
    var vertexArray = new Array[(Long, String)](0)
    var edgeArray = Array(Edge(0L, 0L, "http://example.org/URI"))

    var vertexURIMap = new HashMap[String, Long]
    var nextVertexNum = 0L

    //map triples
    NTripleReader.fromFile(rdfFile).foreach {
      case (subj, pred, obj, lit) => {

        if (!vertexURIMap.contains(subj)) {
          vertexURIMap(subj) = nextVertexNum
          nextVertexNum += 1
        }

        if (!vertexURIMap.contains(pred)) {
          vertexURIMap(pred) = nextVertexNum
          nextVertexNum += 1
        }
        val subjectVertexNumber = vertexURIMap(subj)
        val predicateVertexNumber = vertexURIMap(pred)

        if (lit.isEmpty()) {
          if (!vertexURIMap.contains(obj)) {
            vertexURIMap(obj) = nextVertexNum
            nextVertexNum += 1
          }
          val objectVertexNumber = vertexURIMap(obj)
          edgeArray = edgeArray :+
            Edge(subjectVertexNumber, objectVertexNumber, pred)
        } else {
          literalPropsTriplesArray = literalPropsTriplesArray :+
            (subjectVertexNumber, predicateVertexNumber, lit)
        }
      }
    }

    // Switch value and key for vertexArray that we'll use to create the
    // GraphX graph.
    for ((k, v) <- vertexURIMap) vertexArray :+= (v, k)

    for (i <- 0 until literalPropsTriplesArray.length) {
      if (literalPropsTriplesArray(i)._2 ==
        vertexURIMap("http://www.w3.org/2004/02/skos/core#prefLabel")) {
        // Lose the language tag.
        val prefLabel =
          NTriplesParser.languageTagPattern().replaceFirstIn(literalPropsTriplesArray(i)._3, "")
        prefLabelMap(literalPropsTriplesArray(i)._1) = prefLabel;
      }
    }

    // Create RDDs and Graph from the parsed data.
    // vertexRDD Long: the GraphX longint identifier. String: the URI.
    val vertexRDD: RDD[(Long, String)] = sparkContext.parallelize(vertexArray)

    // edgeRDD String: the URI of the triple predicate.
    val edgeRDD: RDD[Edge[(String)]] =
      sparkContext.parallelize(edgeArray.slice(1, edgeArray.length))

    // literalPropsTriples Long, Long, and String: the subject and predicate
    // vertex numbers and the the literal value that the predicate is
    // associating with the subject.
    val literalPropsTriplesRDD: RDD[(Long, Long, String)] =
      sparkContext.parallelize(literalPropsTriplesArray)

    val graph: Graph[String, String] = Graph(vertexRDD, edgeRDD).cache()

    // Create a subgraph based on the vertices connected by SKOS "related" property
    val skosSubgraph =
      graph.subgraph(t => t.attr ==
        "http://www.w3.org/2004/02/skos/core#related")
    // "http://www.w3.org/2002/07/owl#sameAs")

    // Find connected components  of skosSubgraph.
    val ccGraph = skosSubgraph.connectedComponents()

    //SparkPageRank.apply(ccGraph, 10) //page rank for 10 iterations

    // Fill the componentLists.
    skosSubgraph.vertices.leftJoin(ccGraph.vertices) {
      case (id, u, comp) => comp.get
    }.foreach {
      case (id, startingNode) =>
        {
          // Add id to the list of components with a key
          if (!(componentLists.contains(startingNode))) {
            componentLists(startingNode) = new ListBuffer[VertexId]
          }
          componentLists(startingNode) += id
        }
    }

    /*
    //print labels which contains keyword as character
    literalPropsTriplesRDD.cache()
    literalPropsTriplesRDD.filter(x => { x._3.contains("in") })
      .collect.foreach(println)

    //val vertices = pages.filter(_.title.contains("Leipzig")).map({ v => VertexRDD(pageHash(v.title), v.title) })

    //val writer = new PrintWriter(outputPath)
	*/

    println("------connected components in SKOS \"related\" triples ------\n")
    // writer.write("------connected components in SKOS \"related\" triples ------\n")
    for ((component, componentList) <- componentLists) {
      if (componentList.size > 1) {
        for (c <- componentList) {
          println(prefLabelMap(c));
          //writer.write(prefLabelMap(c) + "\n")
        }
        println("--------------------------")
        //writer.write("--------------------------" + "\n")
      }
    }

    //----- test RDF Model
    val file = "C:/Users/Gezimi/Desktop/AKSW/Spark/sparkworkspace/data/nyse.nt"
    val text = sparkContext.textFile(file)
    text.map(x => newTriple(x))
    val rs = text.map(parseTriple)

    val indexedmap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithIndex //indexing
    val vertices: RDD[(VertexId, String)] = indexedmap.map(x => (x._2, x._1))

    val tuples = rs.keyBy(_._1).join(indexedmap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[String]] = tuples.join(indexedmap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    val graph_nyse = Graph(vertices, edges)

    val pagerank = graph_nyse.pageRank(0.00001).vertices

    val report = pagerank.join(vertices)
      .map({ case (k, (r, v)) => (r, v, k) })
      .sortBy(50 - _._1)

    report.take(50).foreach(println)

    //save praph as JSON
    GraphUtils.saveGraphToJson(graph_nyse, "C:/Users/Gezimi/Desktop/AKSW/Spark/sparkworkspace/data/nyse2.json")

    logger.info("RDFModel..........executed")

    //writer.close()
    sparkContext.stop()
  }

  def readModel(fn: String): Model = {
    val model = ModelFactory.createDefaultModel()
    model.read(fn)
    model
  }

  def parseTriple(fn: String) = {
    val triples = RiotReader.createIteratorTriples(new StringInputStream(fn), Lang.NTRIPLES, "").next
    (triples.getSubject.toString(), triples.getPredicate.toString(), if (triples.getObject.isLiteral()) triples.getObject.getLiteralValue().toString() else triples.getObject().toString())
  }

}
