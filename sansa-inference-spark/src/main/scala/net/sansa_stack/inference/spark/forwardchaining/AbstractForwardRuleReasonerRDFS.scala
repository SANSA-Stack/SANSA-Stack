package net.sansa_stack.inference.spark.forwardchaining

import org.apache.jena.vocabulary.RDF
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import net.sansa_stack.inference.rules.RDFSLevel._
import net.sansa_stack.inference.spark.data.{AbstractRDFGraph, RDFGraph}

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @constructor create a new RDFS forward chaining reasoner
  * @param session the Apache Spark session
  * @param parallelism the level of parallelism
  * @author Lorenz Buehmann
  */
abstract class AbstractForwardRuleReasonerRDFS[T, G <: AbstractRDFGraph[T, G]](session: SparkSession, parallelism: Int = 2)
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

    val r7 = rule7(graph)

    val r2 = rule2(r7)
    val r3 = rule3(r7)
    val r23 = r2.union(r3)

    val types = r23.union(graph.find(None, Some(RDF.`type`.getURI), None))
    val r5 = rule5(graph)
    val r9 = rule9(types)

    // 5. merge triples and remove duplicates
    var allTriples = graph.unionAll(Seq(
      r7,
      r9,
      r23,
      r5
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
