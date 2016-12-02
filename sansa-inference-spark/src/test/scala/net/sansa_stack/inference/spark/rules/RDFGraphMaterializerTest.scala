package net.sansa_stack.inference.spark.rules

import org.apache.jena.rdf.model.ModelFactory
import org.apache.spark.{SparkConf, SparkContext}
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.data.{RDFGraph, RDFGraphWriter}
import net.sansa_stack.inference.spark.forwardchaining.ForwardRuleReasonerRDFS

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

    val graph = RDFGraph(triplesRDD)

    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFS(sc)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    // write triples to disk
    RDFGraphWriter.writeGraphToFile(inferredGraph, args(0))

    sc.stop()


  }
}
