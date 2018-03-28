package net.sansa_stack.rdf.spark.partition.graph.utils

import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Node
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

/**
  * Load a RDF file and transform as object Graph[]
  *
  * @author Zhe Wang
  */
object InitialGraph extends Serializable {
  /**
    * Constructs GraphX graph from loading a RDF file
    * @param session Spark session
    * @param path Path of RDF files
    * @return Graph[Node,Node].
    */
  def apply(session: SparkSession,path: String): Graph[Node,Node]= {
    val reader = NTripleReader.load (session, path) //Currently only support to load N-Triple files
    LoadGraph.apply (reader)
  }

  /**
    * Constructs GraphX graph as String from loading a RDF file
    * @param session Spark session
    * @param path Path of RDF files
    * @return Graph[String,String].
    */
  def applyAsString (session: SparkSession,path: String): Graph[String,String]={
    val reader = NTripleReader.load (session, path) //Currently only support to load N-Triple files
    LoadGraph.asString(reader)
  }
}
