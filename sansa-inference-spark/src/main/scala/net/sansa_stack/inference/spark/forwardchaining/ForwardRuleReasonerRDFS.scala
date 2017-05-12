package net.sansa_stack.inference.spark.forwardchaining

import scala.collection.mutable

import org.apache.jena.graph.Triple
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import net.sansa_stack.inference.rules.RDFSLevel._
import net.sansa_stack.inference.spark.data.model.RDFGraph
import net.sansa_stack.inference.spark.data.model.TripleUtils._
import net.sansa_stack.inference.spark.utils.RDDUtils.RDDOps
import net.sansa_stack.inference.spark.utils.RDFSSchemaExtractor
import net.sansa_stack.inference.utils.CollectionUtils

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @constructor create a new RDFS forward chaining reasoner
  * @param sc the Apache Spark context
  * @param parallelism the level of parallelism
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerRDFS(sc: SparkContext, parallelism: Int = 2) extends TransitiveReasoner(sc, parallelism) {

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  var level: RDFSLevel = DEFAULT

  var extractSchemaTriplesInAdvance: Boolean = true

  override def apply(graph: RDFGraph): RDFGraph = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    var triplesRDD = graph.triples.distinct() // we cache this RDD because it's used quite often
    triplesRDD.cache()
    // RDFS rules dependency was analyzed in \todo(add references) and the same ordering is used here

    // as an optimization, we can extract all schema triples first which avoids to run on the whole dataset
    // for each schema triple later
    val schemaTriples = if (extractSchemaTriplesInAdvance) new RDFSSchemaExtractor().extract(triplesRDD)
                        else triplesRDD


    // 1. we first compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf

    /**
      * rdfs11 xxx rdfs:subClassOf yyy .
      * yyy rdfs:subClassOf zzz . xxx rdfs:subClassOf zzz .
     */
    val subClassOfTriples = extractTriples(schemaTriples, RDFS.subClassOf.asNode()) // extract rdfs:subClassOf triples
    val subClassOfTriplesTrans = computeTransitiveClosure(subClassOfTriples, RDFS.subClassOf.asNode()).setName("rdfs11")// mutable.Set()++subClassOfTriples.collect())

    /*
        rdfs5	xxx rdfs:subPropertyOf yyy .
              yyy rdfs:subPropertyOf zzz .	xxx rdfs:subPropertyOf zzz .
     */
    val subPropertyOfTriples = extractTriples(schemaTriples, RDFS.subPropertyOf.asNode()) // extract rdfs:subPropertyOf triples
    val subPropertyOfTriplesTrans = computeTransitiveClosure(subPropertyOfTriples, RDFS.subPropertyOf.asNode()).setName("rdfs5")// extractTriples(mutable.Set()++subPropertyOfTriples.collect(), RDFS.subPropertyOf.getURI))

    // a map structure should be more efficient
    val subClassOfMap = CollectionUtils.toMultiMap(subClassOfTriplesTrans.map(t => (t.s, t.o)).collect)
    val subPropertyMap = CollectionUtils.toMultiMap(subPropertyOfTriplesTrans.map(t => (t.s, t.o)).collect)

    // distribute the schema data structures by means of shared variables
    // the assumption here is that the schema is usually much smaller than the instance data
    val subClassOfMapBC = sc.broadcast(subClassOfMap)
    val subPropertyMapBC = sc.broadcast(subPropertyMap)

    // split by rdf:type
    val split = triplesRDD.partitionBy(t => t.p == RDF.`type`.asNode)
    var typeTriples = split._1
    var otherTriples = split._2

//    val formatter = java.text.NumberFormat.getIntegerInstance
//    println("triples" + formatter.format(triplesRDD.count()))
//    println("types:" + formatter.format(typeTriples.count()))
//    println("others:" + formatter.format(otherTriples.count()))

    // 2. SubPropertyOf inheritance according to rdfs7 is computed

    /*
      rdfs7	aaa rdfs:subPropertyOf bbb .
            xxx aaa yyy .                   	xxx bbb yyy .
     */
    val triplesRDFS7 =
      otherTriples // all triples (s p1 o)
        .filter(t => subPropertyMapBC.value.contains(t.p)) // such that p1 has a super property p2
        .flatMap(t => subPropertyMapBC.value(t.p)
        .map(supProp => Triple.create(t.s, supProp, t.o))) // create triple (s p2 o)
        .setName("rdfs7")

    // add triples
    otherTriples = otherTriples.union(triplesRDFS7)

    // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

    /*
    rdfs2	aaa rdfs:domain xxx .
          yyy aaa zzz .	          yyy rdf:type xxx .
     */
    val domainTriples = extractTriples(schemaTriples, RDFS.domain.asNode())
    val domainMap = domainTriples.map(t => (t.s, t.o)).collect.toMap
    val domainMapBC = sc.broadcast(domainMap)

    val triplesRDFS2 =
      otherTriples
        .filter(t => domainMapBC.value.contains(t.p))
        .map(t => Triple.create(t.s, RDF.`type`.asNode(), domainMapBC.value(t.p)))
        .setName("rdfs2")

    /*
   rdfs3	aaa rdfs:range xxx .
         yyy aaa zzz .	          zzz rdf:type xxx .
    */
    val rangeTriples = extractTriples(schemaTriples, RDFS.range.asNode())
    val rangeMap = rangeTriples.map(t => (t.s, t.o)).collect().toMap
    val rangeMapBC = sc.broadcast(rangeMap)

    val triplesRDFS3 =
      otherTriples
        .filter(t => rangeMapBC.value.contains(t.p))
        .map(t => Triple.create(t.o, RDF.`type`.asNode(), rangeMapBC.value(t.p)))
        .setName("rdfs3")

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
        .filter(t => subClassOfMapBC.value.contains(t.o)) // such that A has a super class B
        .flatMap(t => subClassOfMapBC.value(t.o).map(supCls => Triple.create(t.s, RDF.`type`.asNode(), supCls))) // create triple (s a B)
        .setName("rdfs9")



//    println("instance_data=" + formatter.format(triplesRDD.count()))
//    println("r7=" + formatter.format(triplesRDFS7.count()))
//    println("r2=" + formatter.format(triplesRDFS2.count()))
//    println("r3=" + formatter.format(triplesRDFS3.count()))
//    println("rdf:type=" + formatter.format(typeTriples.count()))
//    println("r9=" + formatter.format(triplesRDFS9.count()))

//    val tmp = triplesRDFS9.union(typeTriples).distinct(parallelism)
//    println("types=" + formatter.format(tmp.count()))

//    val tmp2 = sc.union(Seq(
//      typeTriples,
//      triplesRDFS9,
//      triplesRDFS7,
//      otherTriples)).distinct(parallelism)
//    println("all=" + formatter.format(tmp2.count()))


    // 5. merge triples and remove duplicates
    var allTriples = sc.union(Seq(
                          otherTriples,
                          subClassOfTriplesTrans,
                          subPropertyOfTriplesTrans,
                          typeTriples,
                          triplesRDFS7,
                          triplesRDFS9))
                        .distinct(parallelism)

    // we perform also additional rules if enabled
    if(level != SIMPLE) {
      // rdfs1

      // rdf1: (s p o) => (p rdf:type rdf:Property)

      // rdfs4a: (s p o) => (s rdf:type rdfs:Resource)
      // rdfs4a: (s p o) => (o rdf:type rdfs:Resource)
      val rdfs4 = allTriples.flatMap(t => Set(
        Triple.create(t.s, RDF.`type`.asNode(), RDFS.Resource.asNode()),
        Triple.create(t.o, RDF.`type`.asNode, RDFS.Resource.asNode)
        //          RDFTriple(t.predicate, RDF.`type`.getURI, RDF.Property.getURI)
      ))

      // rdfs12: (?x rdf:type rdfs:ContainerMembershipProperty) -> (?x rdfs:subPropertyOf rdfs:member)
      val rdfs12 = typeTriples
        .filter(t => t.o == RDFS.ContainerMembershipProperty.asNode)
        .map(t => Triple.create(t.s, RDF.`type`.asNode, RDFS.member.asNode))

      // rdfs6: (p rdf:type rdf:Property) => (p rdfs:subPropertyOf p)
      val rdfs6 = typeTriples
        .filter(t => t.o == RDF.Property.asNode)
        .map(t => Triple.create(t.s, RDFS.subPropertyOf.asNode, t.s))

      // rdfs8: (s rdf:type rdfs:Class ) => (s rdfs:subClassOf rdfs:Resource)
      // rdfs10: (s rdf:type rdfs:Class) => (s rdfs:subClassOf s)
      val rdfs8_10 = typeTriples
        .filter(t => t.o == RDFS.Class.asNode)
        .flatMap(t => Set(
          Triple.create(t.s, RDFS.subClassOf.asNode, RDFS.Resource.asNode),
          Triple.create(t.s, RDFS.subClassOf.asNode, t.s)))

      val additionalTripleRDDs = mutable.Seq(rdfs4, rdfs6, rdfs8_10, rdfs12)

      allTriples = sc.union(Seq(allTriples) ++ additionalTripleRDDs).distinct(parallelism)
    }

    logger.info("...finished materialization in " + (System.currentTimeMillis() - startTime) + "ms.")
//    val newSize = allTriples.count()
//    logger.info(s"|G_inf|=$newSize")

    // return graph with inferred triples
    RDFGraph(allTriples)
  }
}
