package net.sansa_stack.inference.spark.utils

import org.apache.jena.vocabulary.RDFS
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import net.sansa_stack.inference.spark.data.model.{RDFGraph, RDFGraphDataFrame, RDFGraphNative}
import net.sansa_stack.inference.utils.{CollectionUtils, Logging}

import org.apache.jena.graph.{Node, Triple}
import net.sansa_stack.inference.spark.data.model.TripleUtils._

/**
  * An extractor of the schema for RDFS.
  *
  * Currently, it's supports the extraction of triples `(s,p,o)` with `p` being
  *
  *  - rdfs:subClassOf
  *  - rdfs:subPropertyOf
  *  - rdfs:domain
  *  - rdfs:range
  *
  * @author Lorenz Buehmann
  */
class RDFSSchemaExtractor() extends Logging with Serializable {

  val properties = Set(RDFS.subClassOf, RDFS.subPropertyOf, RDFS.domain, RDFS.range).map(p => p.asNode())

  /**
    * Extracts the RDF graph containing only the schema triples from the RDF graph.
    *
    * @param graph the RDF graph
    * @return the RDF graph containing only the schema triples
    */
  def extract(graph: RDFGraph): RDFGraph = {
    log.info("Started schema extraction...")

    val filteredTriples = graph.triples.filter(t => properties.contains(t.p))

    log.info("Finished schema extraction.")

    new RDFGraph(filteredTriples)
  }

  /**
    * Extracts the schema triples from the given triples.
    *
    * @param triples the triples
    * @return the schema triples
    */
  def extract(triples: RDD[Triple]): RDD[Triple] = {
    log.info("Started schema extraction...")

    val filteredTriples = triples.filter(t => properties.contains(t.p))

    log.info("Finished schema extraction.")

    filteredTriples
  }


  /**
    * Computes the s-o pairs for each schema property p, e.g. `rdfs:subClassOf` and returns it as mapping from p
    * to the RDD of s-o pairs.
    *
    * @param graph the RDF graph
    * @return a mapping from the corresponding schema property to the RDD of s-o pairs
    */
  def extractWithIndex(graph: RDFGraphNative): Map[Node, RDD[(Node, Node)]] = {
    log.info("Started schema extraction...")

    // for each schema property p
    val index =
      properties.map { p =>
        // get triples (s,p,o)
        val newGraph = graph.find(None, Some(p), None)

        // map to (s,o)
        val pairs = newGraph.triples.map(t => (t.s, t.o))

        // add to index
        (p -> pairs)
      }
    log.info("Finished schema extraction.")

    index.toMap
  }

  /**
    * Computes the s-o pairs for each schema property p, e.g. `rdfs:subClassOf` and returns it as mapping from p
    * to the Dataframe containing s and o.
    *
    * @param graph the RDF graph
    * @return a mapping from the corresponding schema property to the Dataframe of s-o pairs
    */
  def extractWithIndex(graph: RDFGraphDataFrame): Map[Node, DataFrame] = {
    log.info("Started schema extraction...")

    // for each schema property p
    val index =
      properties.map { p =>
        // get triples (s,p,o)
        val newGraph = graph.find(None, Some(p.getURI), None)

        // map to (s,o)
        val pairs = newGraph.triples.select(graph.schema.subjectCol, graph.schema.predicateCol, graph.schema.objectCol)

        // add to index
        (p -> pairs)
      }
    log.info("Finished schema extraction.")

    index.toMap
  }

  /**
    * Computes the s-o pairs for each schema property, e.g. `rdfs:subClassOf` and distributes it to all worker
    * nodes via broadcast.
    *
    * @param graph the RDF graph
    * @return a mapping from the corresponding schema property to the broadcast variable that wraps the multimap
    *         with s-o pairs
    */
  def extractWithIndexAndDistribute(sc : SparkContext, graph: RDFGraphNative): Map[Node, Broadcast[Map[Node, Set[Node]]]] = {
    val schema = extractWithIndex(graph)

    log.info("Started schema distribution...")
    val index =
      schema.map { e =>
        val p = e._1
        val rdd = e._2

        // we can't call RDD::collectAsMap because it does not support multimaps
        val mmap = CollectionUtils.toMultiMap(rdd.collect())

        // broadcast
        val bv = sc.broadcast(mmap)

        // add to index
        (p -> bv)
      }
    log.info("Finished schema distribution.")

    index
  }

}


