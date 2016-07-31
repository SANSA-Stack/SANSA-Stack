package org.dissect.inference.rules

import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.dissect.inference.data.{RDFGraph, RDFGraphWriter, RDFTriple}
import org.dissect.inference.forwardchaining.{ForwardRuleReasonerOWLHorst, ForwardRuleReasonerRDFS}

import scala.collection.mutable

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

    val triples = new mutable.HashSet[RDFTriple]()
    val iter = m.listStatements()
    while(iter.hasNext) {
      val st = iter.next()
      triples.add(RDFTriple(st.getSubject.toString, st.getPredicate.toString, st.getObject.toString))
    }


    val triplesRDD = sc.parallelize(triples.toSeq, 2)

    val graph = new RDFGraph(triplesRDD)

    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFS(sc)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    // write triples to disk
    RDFGraphWriter.writeToFile(inferredGraph, args(0))

    sc.stop()


  }
}
