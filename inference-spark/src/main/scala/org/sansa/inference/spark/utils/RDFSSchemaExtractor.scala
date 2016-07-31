package org.dissect.inference.utils

import org.apache.jena.vocabulary.RDFS
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dissect.inference.data._
import org.dissect.inference.utils.logging.Logging
import org.sansa.inference.spark.data.RDFGraphDataFrame

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
class RDFSSchemaExtractor(session : SparkSession) extends Logging{

  val properties = List(RDFS.subClassOf, RDFS.subPropertyOf, RDFS.domain, RDFS.range).map(p => p.getURI)


  /**
    * Computes the s-o pairs for each schema property p, e.g. `rdfs:subClassOf` and returns it as mapping from p
    * to the RDD of s-o pairs.
    *
    * @param graph the RDF graph
    * @return a mapping from the corresponding schema property to the RDD of s-o pairs
    */
  def extract(graph: RDFGraphNative): Map[String, RDD[(String, String)]] = {
    log.info("Started schema extraction...")

    // for each schema property p
    val index =
      properties.map { p =>
        // get triples (s,p,o)
        val triples = graph.find(None, Some(p), None)

        // map to (s,o)
        val pairs = triples.map(t => (t.s, t.o))

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
  def extract(graph: RDFGraphDataFrame): Map[String, DataFrame] = {
    log.info("Started schema extraction...")

    // for each schema property p
    val index =
      properties.map { p =>
        // get triples (s,p,o)
        val triples = graph.find(None, Some(p), None)

        // map to (s,o)
        val pairs = triples.select("subject", "predicate", "object")

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
  def extractAndDistribute(graph: RDFGraphNative): Map[String, Broadcast[Map[String, Set[String]]]] = {
    val schema = extract(graph)

    log.info("Started schema distribution...")
    val index =
      schema.map { e =>
        val p = e._1
        val rdd = e._2

        // we can't call RDD::collectAsMap because it does not support multimaps
        val mmap = CollectionUtils.toMultiMap(rdd.collect())

        // broadcast
        val bv = session.sparkContext.broadcast(mmap)

        // add to index
        (p -> bv)
      }
    log.info("Finished schema distribution.")

    index
  }

}


