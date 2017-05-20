package net.sansa_stack.inference.spark.forwardchaining

import org.apache.jena.vocabulary.{RDF => JenaRDF}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import net.sansa_stack.inference.data.RDF
import net.sansa_stack.inference.rules.RDFSLevel._
import net.sansa_stack.inference.spark.data.model.AbstractRDFGraphSpark

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @constructor create a new RDFS forward chaining reasoner
  * @param session the Apache Spark session
  * @param parallelism the level of parallelism
  * @author Lorenz Buehmann
  */
abstract class AbstractForwardRuleReasonerRDFS[D[T], N <: RDF#Node, T <: RDF#Triple, G <: AbstractRDFGraphSpark[D, N, T, G]]
(session: SparkSession, parallelism: Int = 2)
  extends TransitiveReasoner(session.sparkContext, parallelism) {

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  var level: RDFSLevel = DEFAULT

  def rule2(graph: G): G
  def rule3(graph: G): G
  def rule5(graph: G): G
  def rule7(graph: G): G
  def rule9(graph: G): G
  def rule11(graph: G): G

  def preprocess(graph: G): G
  def postprocess(graph: G): G

  def apply(graph: G): G = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    graph.cache()

    preprocess(graph)

    // split into rdf:type triples and other instance data
    var types = graph.find(None, Some(JenaRDF.`type`.getURI), None)
    var others = graph.find(None, Some("!" + JenaRDF.`type`.getURI), None)

//    println("triples:" + graph.size())
//    println("types:" + types.size())
//    println("others:" + others.size())

    /*
        rdfs5	xxx rdfs:subPropertyOf yyy .
              yyy rdfs:subPropertyOf zzz .	xxx rdfs:subPropertyOf zzz .
     */
    val r5 = rule5(graph)

    /*
     rdfs7	aaa rdfs:subPropertyOf bbb .
           xxx aaa yyy .                   	xxx bbb yyy .
    */
    val r7 = rule7(others)
    others = others.union(r7)

    val r2 = rule2(others)
    val r3 = rule3(others)
    val r23 = r2.union(r3)

    types = r23.union(types)


    val r11 = rule11(graph)

    /*
      rdfs9	xxx rdfs:subClassOf yyy .
         zzz rdf:type xxx .	        zzz rdf:type yyy .
    */
    val r9 = rule9(types)

//    println("r7:" + r7.size())
//    println("r2:" + r2.size())
//    println("r3:" + r3.size())
//    println("types:" + types.size())
//    println("r5:" + r5.size())
//    println("r9:" + r9.size())
//    println("types=" + r9.union(types).distinct().size())

    // 5. merge triples and remove duplicates
    var allTriples = r9.unionAll(Seq(
      r9,
      types,
      r5,
      r11,
      others
    ))
      .distinct()

    postprocess(graph)

    logger.info("...finished materialization in " + (System.currentTimeMillis() - startTime) + "ms.")
//    val newSize = allTriples.count()
//    logger.info(s"|G_inf|=$newSize")

    // return graph with inferred triples
    allTriples
  }
}
