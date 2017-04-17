package net.sansa_stack.inference.flink.forwardchaining

import net.sansa_stack.inference.flink.data.RDFGraph
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.jena.vocabulary.{RDF, RDFS}
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.utils.CollectionUtils
import org.slf4j.LoggerFactory
import net.sansa_stack.inference.flink.utils.DataSetUtils.DataSetOps
import net.sansa_stack.inference.rules.RDFSLevel._

import scala.collection.mutable

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @constructor create a new RDFS forward chaining reasoner
  * @param env the Apache Flink execution environment
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerRDFS(env: ExecutionEnvironment)
    extends ForwardRuleReasoner {

  private val logger = com.typesafe.scalalogging
    .Logger(LoggerFactory.getLogger(this.getClass.getName))

  var level: RDFSLevel = DEFAULT

  def apply(graph: RDFGraph): RDFGraph = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    val triplesDS = graph.triples

    // RDFS rules dependency was analyzed in \todo(add references) and the same ordering is used here

    // 1. we first compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf

    /**
      * rdfs11 xxx rdfs:subClassOf yyy .
      * yyy rdfs:subClassOf zzz . xxx rdfs:subClassOf zzz .
      */
    val subClassOfTriples = extractTriples(triplesDS, RDFS.subClassOf.getURI)
      .name("rdfs:subClassOf") // extract rdfs:subClassOf triples
    val subClassOfTriplesTrans =
      computeTransitiveClosureOpt(subClassOfTriples).name("rdfs11")

    /*
        rdfs5	xxx rdfs:subPropertyOf yyy .
              yyy rdfs:subPropertyOf zzz .	xxx rdfs:subPropertyOf zzz .
     */
    val subPropertyOfTriples =
      extractTriples(triplesDS, RDFS.subPropertyOf.getURI)
        .name("rdfs:subPropertyOf") // extract rdfs:subPropertyOf triples
    val subPropertyOfTriplesTrans =
      computeTransitiveClosureOpt(subPropertyOfTriples).name("rdfs5")

    // a map structure should be more efficient
    val subClassOfMap = CollectionUtils.toMultiMap(
      subClassOfTriplesTrans.map(t => (t.s, t.o)).collect)
    val subPropertyMap = CollectionUtils.toMultiMap(
      subPropertyOfTriplesTrans.map(t => (t.s, t.o)).collect)

    // split by rdf:type
    val split = triplesDS.partitionBy(t => t.p == RDF.`type`.getURI)
    var typeTriples = split._1
    var otherTriples = split._2

    // 2. SubPropertyOf inheritance according to rdfs7 is computed

    /*
      rdfs7	aaa rdfs:subPropertyOf bbb .
            xxx aaa yyy .                   	xxx bbb yyy .
     */
    val triplesRDFS7 =
      otherTriples // all triples (s p1 o)
        .filter(t => subPropertyMap.contains(t.p)) // such that p1 has a super property p2
        .flatMap(t =>
          subPropertyMap(t.p).map(supProp => RDFTriple(t.s, supProp, t.o))) // create triple (s p2 o)
        .name("rdfs7")

    // add triples
    otherTriples = otherTriples.union(triplesRDFS7)

    // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

    /*
    rdfs2	aaa rdfs:domain xxx .
          yyy aaa zzz .	          yyy rdf:type xxx .
     */
    val domainTriples = extractTriples(triplesDS, RDFS.domain.getURI).name("rdfs:domain")
    val domainMap = domainTriples.map(t => (t.s, t.o)).collect.toMap

    val triplesRDFS2 =
      otherTriples
        .filter(t => domainMap.contains(t.p))
        .map(t => RDFTriple(t.s, RDF.`type`.getURI, domainMap(t.p)))
        .name("rdfs2")
    /*
   rdfs3	aaa rdfs:range xxx .
         yyy aaa zzz .	          zzz rdf:type xxx .
     */
    val rangeTriples = extractTriples(triplesDS, RDFS.range.getURI).name("rdfs:range")
    val rangeMap = rangeTriples.map(t => (t.s, t.o)).collect().toMap

    val triplesRDFS3 =
      otherTriples
        .filter(t => rangeMap.contains(t.p))
        .map(t => RDFTriple(t.o, RDF.`type`.getURI, rangeMap(t.p)))
        .name("rdfs3")
    // rdfs2 and rdfs3 generated rdf:type triples which we'll add to the existing ones
    val triples23 = triplesRDFS2.union(triplesRDFS3)

    // all rdf:type triples here as intermediate result
    typeTriples = typeTriples.union(triples23)

    // 4. SubClass inheritance according to rdfs9

    /*
    rdfs9	xxx rdfs:subClassOf yyy .
          zzz rdf:type xxx .	        zzz rdf:type yyy .
     */
    val triplesRDFS9 =
      typeTriples // all rdf:type triples (s a A)
        .filter(t => subClassOfMap.contains(t.o)) // such that A has a super class B
        .flatMap(t =>
          subClassOfMap(t.o).map(supCls =>
            RDFTriple(t.s, RDF.`type`.getURI, supCls))) // create triple (s a B)
        .name("rdfs9")

    // 5. merge triples and remove duplicates
    var allTriples = env
      .union(
        Seq(otherTriples,
            subClassOfTriplesTrans,
            subPropertyOfTriplesTrans,
            typeTriples,
            triplesRDFS7,
            triplesRDFS9))
      .distinct()

    // we perform also additional rules if enabled
    if (level != SIMPLE) {
      // rdfs1

      // rdf1: (s p o) => (p rdf:type rdf:Property)

      // rdfs4a: (s p o) => (s rdf:type rdfs:Resource)
      // rdfs4a: (s p o) => (o rdf:type rdfs:Resource)
      val rdfs4 = allTriples
        .flatMap(
          t =>
            Set(
              RDFTriple(t.s, RDF.`type`.getURI, RDFS.Resource.getURI),
              RDFTriple(t.o, RDF.`type`.getURI, RDFS.Resource.getURI)
              //          RDFTriple(t.predicate, RDF.`type`.getURI, RDF.Property.getURI)
          ))
        .name("rdfs4")

      // rdfs12: (?x rdf:type rdfs:ContainerMembershipProperty) -> (?x rdfs:subPropertyOf rdfs:member)
      val rdfs12 = typeTriples
        .filter(t => t.o == RDFS.ContainerMembershipProperty.getURI)
        .map(t => RDFTriple(t.s, RDF.`type`.getURI, RDFS.member.getURI))
        .name("rdfs12")

      // rdfs6: (p rdf:type rdf:Property) => (p rdfs:subPropertyOf p)
      val rdfs6 = typeTriples
        .filter(t => t.o == RDF.Property.getURI)
        .map(t => RDFTriple(t.s, RDFS.subPropertyOf.getURI, t.s))
        .name("rdfs6")

      // rdfs8: (s rdf:type rdfs:Class ) => (s rdfs:subClassOf rdfs:Resource)
      // rdfs10: (s rdf:type rdfs:Class) => (s rdfs:subClassOf s)
      val rdfs8_10 = typeTriples
        .filter(t => t.o == RDFS.Class.getURI)
        .flatMap(t =>
          Set(RDFTriple(t.s, RDFS.subClassOf.getURI, RDFS.Resource.getURI),
              RDFTriple(t.s, RDFS.subClassOf.getURI, t.s)))
        .name("rdfs8/rdfs10")

      val additionalTripleRDDs = mutable.Seq(rdfs4, rdfs6, rdfs8_10)

      allTriples =
        env.union(Seq(allTriples) ++ additionalTripleRDDs).distinct()
    }

    logger.info(
      "...finished materialization in " + (System
        .currentTimeMillis() - startTime) + "ms.")
//    val newSize = allTriples.count()
//    logger.info(s"|G_inf|=$newSize")

    // return graph with inferred triples
    RDFGraph(allTriples)
  }
}
