package net.sansa_stack.inference.spark.rules

import scala.collection.mutable

import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.{SparkConf, SparkContext}

import net.sansa_stack.inference.spark.data.model.RDFGraph
import net.sansa_stack.inference.spark.data.writer.RDFGraphWriter
import net.sansa_stack.inference.spark.forwardchaining.triples.ForwardRuleReasonerRDFS

/**
  * The class to compute the materialization of a given RDF graph.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphMaterializerTest {

  def main(args: Array[String]) {
    // the SPARK config
    val conf = new SparkConf().setAppName("SPARK Reasoning")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.setMaster("local[2]")
    conf.set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)

    val m = ModelFactory.createDefaultModel()
    m.read(RDFGraphMaterializerTest.getClass.getClassLoader.getResourceAsStream("data/owl-horst-minimal2.ttl"), null, "TURTLE")

    val triples = new mutable.HashSet[Triple]()
    val iter = m.listStatements()
    while(iter.hasNext) {
      val st = iter.next()
      triples.add(st.asTriple())
    }


    val triplesRDD = sc.parallelize(triples.toSeq, 2)

    val graph = RDFGraph(triplesRDD)

    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFS(sc)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    // write triples to disk
    RDFGraphWriter.writeToDisk(inferredGraph, args(0))

    sc.stop()


  }
}
