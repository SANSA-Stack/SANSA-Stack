package net.sansa_stack.inference.spark.forwardchaining

import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.SparkContext
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.data.RDFGraph
import net.sansa_stack.inference.utils.CollectionUtils
import org.slf4j.LoggerFactory

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @constructor create a new RDFS forward chaining reasoner
  * @param sc the Apache Spark context
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerRDFS(sc: SparkContext) extends ForwardRuleReasoner{

  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def apply(graph: RDFGraph): RDFGraph = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    var triplesRDD = graph.triples // we cache this RDD because it's used quite often
    triplesRDD.cache()
    // RDFS rules dependency was analyzed in \todo(add references) and the same ordering is used here


    // 1. we first compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf

    /**
      * rdfs11	xxx rdfs:subClassOf yyy .
      * yyy rdfs:subClassOf zzz .	  xxx rdfs:subClassOf zzz .
     */
    val subClassOfTriples = extractTriples(triplesRDD, RDFS.subClassOf.getURI) // extract rdfs:subClassOf triples
    val subClassOfTriplesTrans = computeTransitiveClosure(subClassOfTriples)//mutable.Set()++subClassOfTriples.collect())

    /*
        rdfs5	xxx rdfs:subPropertyOf yyy .
              yyy rdfs:subPropertyOf zzz .	xxx rdfs:subPropertyOf zzz .
     */
    val subPropertyOfTriples = extractTriples(triplesRDD, RDFS.subPropertyOf.getURI) // extract rdfs:subPropertyOf triples
    val subPropertyOfTriplesTrans = computeTransitiveClosure(subPropertyOfTriples)//extractTriples(mutable.Set()++subPropertyOfTriples.collect(), RDFS.subPropertyOf.getURI))

    // a map structure should be more efficient
    val subClassOfMap = CollectionUtils.toMultiMap(subClassOfTriplesTrans.map(t => (t.subject, t.`object`)).collect)
    val subPropertyMap = CollectionUtils.toMultiMap(subPropertyOfTriplesTrans.map(t => (t.subject, t.`object`)).collect)

    // distribute the schema data structures by means of shared variables
    // the assumption here is that the schema is usually much smaller than the instance data
    val subClassOfMapBC = sc.broadcast(subClassOfMap)
    val subPropertyMapBC = sc.broadcast(subPropertyMap)


    // 2. SubPropertyOf inheritance according to rdfs7 is computed

    /*
      rdfs7	aaa rdfs:subPropertyOf bbb .
            xxx aaa yyy .                   	xxx bbb yyy .
     */
    val triplesRDFS7 =
      triplesRDD // all triples (s p1 o)
      .filter(t => subPropertyMapBC.value.contains(t.predicate)) // such that p1 has a super property p2
      .flatMap(t => subPropertyMapBC.value(t.predicate).map(supProp => RDFTriple(t.subject, supProp, t.`object`))) // create triple (s p2 o)

    // add triples
    triplesRDD = triplesRDD.union(triplesRDFS7)

    // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

    /*
    rdfs2	aaa rdfs:domain xxx .
          yyy aaa zzz .	          yyy rdf:type xxx .
     */
    val domainTriples = extractTriples(triplesRDD, RDFS.domain.getURI)
    val domainMap = domainTriples.map(t => (t.subject, t.`object`)).collect.toMap
    val domainMapBC = sc.broadcast(domainMap)

    val triplesRDFS2 =
      triplesRDD
        .filter(t => domainMapBC.value.contains(t.predicate))
        .map(t => RDFTriple(t.subject, RDF.`type`.getURI, domainMapBC.value(t.predicate)))

    /*
   rdfs3	aaa rdfs:range xxx .
         yyy aaa zzz .	          zzz rdf:type xxx .
    */
    val rangeTriples = extractTriples(triplesRDD, RDFS.range.getURI)
    val rangeMap = rangeTriples.map(t => (t.subject, t.`object`)).collect().toMap
    val rangeMapBC = sc.broadcast(rangeMap)

    val triplesRDFS3 =
      triplesRDD
        .filter(t => rangeMapBC.value.contains(t.predicate))
        .map(t => RDFTriple(t.`object`, RDF.`type`.getURI, rangeMapBC.value(t.predicate)))

    val triples23 = triplesRDFS2.union(triplesRDFS3)

    // get rdf:type tuples here as intermediate result
    val typeTriples = triplesRDD
      .filter(t => t.predicate == RDF.`type`.getURI)
      .union(triples23)

    // 4. SubClass inheritance according to rdfs9

    /*
    rdfs9	xxx rdfs:subClassOf yyy .
          zzz rdf:type xxx .	        zzz rdf:type yyy .
     */
    val triplesRDFS9 =
      typeTriples // all rdf:type triples (s a A)
        .filter(t => subClassOfMapBC.value.contains(t.`object`)) // such that A has a super class B
        .flatMap(t => subClassOfMapBC.value(t.`object`).map(supCls => RDFTriple(t.subject, RDF.`type`.getURI, supCls))) // create triple (s a B)

    // 5. merge triples and remove duplicates
    val allTriples = sc.union(Seq(
                          subClassOfTriplesTrans,
                          subPropertyOfTriplesTrans,
                          triples23,
                          triplesRDFS7,
                          triplesRDFS9))
                        .distinct()

    logger.info("...finished materialization in " + (System.currentTimeMillis() - startTime) + "ms.")
    val newSize = allTriples.count()
    logger.info(s"|G_inf|=$newSize")

    // return graph with inferred triples
    new RDFGraph(allTriples)
  }
}
