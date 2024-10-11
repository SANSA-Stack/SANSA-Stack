package net.sansa_stack.inference.flink.forwardchaining

import scala.jdk.CollectionConverters._
import scala.collection.mutable

import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.jena.graph.Triple
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.slf4j.LoggerFactory

import net.sansa_stack.inference.flink.data.RDFGraph
import net.sansa_stack.inference.flink.extraction.RDFSSchemaExtractor
import net.sansa_stack.inference.flink.utils.DataSetUtils.DataSetOps
import net.sansa_stack.inference.rules.RDFSLevel._
import net.sansa_stack.inference.utils.CollectionUtils

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @constructor create a new RDFS forward chaining reasoner
  * @param env the Apache Flink execution environment
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerRDFS(env: ExecutionEnvironment) extends ForwardRuleReasoner {

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  var level: RDFSLevel = DEFAULT

  var extractSchemaTriplesInAdvance: Boolean = true
  var useSchemaBroadCasting: Boolean = false

  def apply(graph: RDFGraph): RDFGraph = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    val triplesDS = graph.triples

    // RDFS rules dependency was analyzed in \todo(add references) and the same ordering is used here

    // as an optimization, we can extract all schema triples first which avoids to run on the whole dataset
    // for each schema triple later
    val schemaTriples =
      if (extractSchemaTriplesInAdvance) new RDFSSchemaExtractor().extract(triplesDS)
      else triplesDS

    // 1. we first compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf

    /**
      * rdfs11 xxx rdfs:subClassOf yyy .
      * yyy rdfs:subClassOf zzz . xxx rdfs:subClassOf zzz .
      */
    val subClassOfTriples =
      extractTriples(schemaTriples, RDFS.subClassOf.asNode())
        .name("rdfs:subClassOf") // extract rdfs:subClassOf triples
    val subClassOfTriplesTrans =
      computeTransitiveClosureOptSemiNaive(subClassOfTriples, RDFS.subClassOf.asNode()).name("rdfs11")

    /*
        rdfs5 xxx rdfs:subPropertyOf yyy .
              yyy rdfs:subPropertyOf zzz . => xxx rdfs:subPropertyOf zzz .
     */
    val subPropertyOfTriples =
      extractTriples(schemaTriples, RDFS.subPropertyOf.asNode())
        .name("rdfs:subPropertyOf") // extract rdfs:subPropertyOf triples
    val subPropertyOfTriplesTrans =
      computeTransitiveClosureOptSemiNaive(subPropertyOfTriples, RDFS.subPropertyOf.asNode()).name("rdfs5")

    // split by rdf:type
    val split = triplesDS.partitionBy(t => t.predicateMatches(RDF.`type`.asNode()))
    var typeTriples = split._1.name("rdf:type triples")
    var otherTriples = split._2

    // 2. SubPropertyOf inheritance according to rdfs7 is computed

    /*
      rdfs7 aaa rdfs:subPropertyOf bbb .
            xxx aaa yyy .                 => xxx bbb yyy .
     */
    val triplesRDFS7 = if (useSchemaBroadCasting) {
      otherTriples
        .filter(new RichFilterFunction[Triple]() {

          var broadcastSet: Traversable[Triple] = _

          override def open(config: Configuration): Unit = {
            broadcastSet = getRuntimeContext.getBroadcastVariable[Triple]("subPropertyTriples").asScala
          }

          override def filter(t: Triple): Boolean = broadcastSet.exists(_.subjectMatches(t.getPredicate))
        })
        .withBroadcastSet(subPropertyOfTriplesTrans, "subPropertyTriples")
        //        .flatMap(new SubClassOfFlatMapFunction("subClasses")).withBroadcastSet(subClassOfTriplesTrans, "subClasses") // create triple (s a B)
        .flatMap(new RichFlatMapFunction[Triple, Triple]() {
          var broadcastSet: Traversable[Triple] = _

          override def open(config: Configuration): Unit = {
            broadcastSet = getRuntimeContext.getBroadcastVariable[Triple]("subPropertyTriples").asScala
          }

          override def flatMap(in: Triple, collector: Collector[Triple]): Unit = {
            broadcastSet
              .filter(_.subjectMatches(in.getPredicate))
              .foreach(t => collector.collect(Triple.create(in.getSubject, t.getObject, in.getObject)))
          }
        })
        .withBroadcastSet(subPropertyOfTriplesTrans, "subPropertyTriples")
    } else {
      val subPropertyMap =
        CollectionUtils.toMultiMap(subPropertyOfTriplesTrans.map(t => (t.getSubject, t.getObject)).collect)

      otherTriples // all triples (s p1 o)
        .filter(t => subPropertyMap.contains(t.getPredicate)) // such that p1 has a super property p2
        .flatMap(t => subPropertyMap(t.getPredicate).map(supProp => Triple.create(t.getSubject, supProp, t.getObject))) // create triple (s p2 o)

    }.name("rdfs7")

    // add triples
    otherTriples = otherTriples.union(triplesRDFS7)

    // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

    /*
    rdfs2 aaa rdfs:domain xxx .
          yyy aaa zzz .           => yyy rdf:type xxx .
     */
    val domainTriples =
      extractTriples(schemaTriples, RDFS.domain.asNode()).name("rdfs:domain")

    val triplesRDFS2 = if (useSchemaBroadCasting) {
      otherTriples
        .filter(new RichFilterFunction[Triple]() {

          var broadcastSet: Traversable[Triple] = _

          override def open(config: Configuration): Unit = {
            broadcastSet = getRuntimeContext.getBroadcastVariable[Triple]("domainTriples").asScala
          }

          override def filter(t: Triple): Boolean =
            broadcastSet.exists(_.subjectMatches(t.getPredicate))
        })
        .withBroadcastSet(domainTriples, "domainTriples")
        .flatMap(new RichFlatMapFunction[Triple, Triple]() {
          var broadcastSet: Traversable[Triple] = _

          override def open(config: Configuration): Unit = {
            broadcastSet = getRuntimeContext.getBroadcastVariable[Triple]("domainTriples").asScala
          }

          override def flatMap(in: Triple, collector: Collector[Triple]): Unit = {
            broadcastSet
              .filter(_.subjectMatches(in.getPredicate))
              .foreach(t => collector.collect(Triple.create(in.getSubject, RDF.`type`.asNode(), t.getObject)))
          }
        })
        .withBroadcastSet(domainTriples, "domainTriples")
    } else {
      val domainMap = domainTriples.map(t => (t.getSubject, t.getObject)).collect.toMap

      otherTriples
        .filter(t => domainMap.contains(t.getPredicate))
        .map(t => Triple.create(t.getSubject, RDF.`type`.asNode(), domainMap(t.getPredicate)))

    }.name("rdfs2")

    /*
   rdfs3 aaa rdfs:range xxx .
         yyy aaa zzz .           => zzz rdf:type xxx .
     */
    val rangeTriples =
      extractTriples(schemaTriples, RDFS.range.asNode()).name("rdfs:range")

    val triplesRDFS3 = if (useSchemaBroadCasting) {
      otherTriples
        .filter(new RichFilterFunction[Triple]() {

          var broadcastSet: Traversable[Triple] = _

          override def open(config: Configuration): Unit = {
            broadcastSet = getRuntimeContext.getBroadcastVariable[Triple]("rangeTriples").asScala
          }

          override def filter(t: Triple): Boolean =
            broadcastSet.exists(_.subjectMatches(t.getPredicate))
        })
        .withBroadcastSet(rangeTriples, "rangeTriples")
        .flatMap(new RichFlatMapFunction[Triple, Triple]() {
          var broadcastSet: Traversable[Triple] = _

          override def open(config: Configuration): Unit = {
            broadcastSet = getRuntimeContext.getBroadcastVariable[Triple]("rangeTriples").asScala
          }

          override def flatMap(in: Triple, collector: Collector[Triple]): Unit = {
            broadcastSet
              .filter(_.subjectMatches(in.getPredicate))
              .foreach(t => collector.collect(Triple.create(in.getObject, RDF.`type`.asNode(), t.getObject)))
          }
        })
        .withBroadcastSet(rangeTriples, "rangeTriples")
    } else {
      val rangeMap = rangeTriples.map(t => (t.getSubject, t.getObject)).collect().toMap

      otherTriples
        .filter(t => rangeMap.contains(t.getPredicate))
        .map(t => Triple.create(t.getObject, RDF.`type`.asNode(), rangeMap(t.getPredicate)))

    }.name("rdfs3")

    // rdfs2 and rdfs3 generated rdf:type triples which we'll add to the existing ones
    val triples23 = triplesRDFS2.union(triplesRDFS3).name("inferred rdf:type triples")

    // all rdf:type triples here as intermediate result
    typeTriples = typeTriples.union(triples23).name("all rdf:type triples")

    // 4. SubClass inheritance according to rdfs9

    /*
    rdfs9 xxx rdfs:subClassOf yyy .
          zzz rdf:type xxx .         => zzz rdf:type yyy .
     */
    val triplesRDFS9 = if (useSchemaBroadCasting) {
      typeTriples // all rdf:type triples (s a A)
      //        .filter(new SubClassOfFilterFunction("subClasses")).withBroadcastSet(subClassOfTriplesTrans, "subClasses") // such that A has a super class B
        .filter(new RichFilterFunction[Triple]() {

          var broadcastSet: Traversable[Triple] = _

          override def open(config: Configuration): Unit = {
            broadcastSet = getRuntimeContext.getBroadcastVariable[Triple]("subClassTriples").asScala
          }

          override def filter(t: Triple): Boolean =
            broadcastSet.exists(_.subjectMatches(t.getObject))
        })
        .withBroadcastSet(subClassOfTriplesTrans, "subClassTriples")
        //        .flatMap(new SubClassOfFlatMapFunction("subClasses")).withBroadcastSet(subClassOfTriplesTrans, "subClasses") // create triple (s a B)
        .flatMap(new RichFlatMapFunction[Triple, Triple]() {
          var broadcastSet: Traversable[Triple] = _

          override def open(config: Configuration): Unit = {
            broadcastSet = getRuntimeContext.getBroadcastVariable[Triple]("subClassTriples").asScala
          }

          override def flatMap(in: Triple, collector: Collector[Triple]): Unit = {
            broadcastSet
              .filter(t => in.objectMatches(t.getSubject))
              .foreach(t => collector.collect(Triple.create(in.getSubject, in.getPredicate, t.getObject)))
          }
        })
        .withBroadcastSet(subClassOfTriplesTrans, "subClassTriples")
    } else {
      val subClassOfMap = CollectionUtils.toMultiMap(subClassOfTriplesTrans.map(t => (t.getSubject, t.getObject)).collect)

      typeTriples // all rdf:type triples (s a A)
        .filter(t => subClassOfMap.contains(t.getObject)) // such that A has a super class B
        .flatMap(
          t =>
            subClassOfMap(t.getObject)
              .map(supCls => Triple.create(t.getSubject, RDF.`type`.asNode(), supCls))
        ) // create triple (s a B)
    }.name("rdfs9")

    // 5. merge triples and remove duplicates
    var allTriples = env
      .union(
        Seq(otherTriples, subClassOfTriplesTrans, subPropertyOfTriplesTrans, typeTriples, triplesRDFS7, triplesRDFS9)
      )
      .distinct(t => t.hashCode())

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
              Triple.create(t.getSubject, RDF.`type`.asNode(), RDFS.Resource.asNode()),
              Triple.create(t.getObject, RDF.`type`.asNode(), RDFS.Resource.asNode())
              //          Triple(t.predicate, RDF.`type`.getURI, RDF.Property.getURI)
          )
        )
        .name("rdfs4")

      // rdfs12: (?x rdf:type rdfs:ContainerMembershipProperty) -> (?x rdfs:subPropertyOf rdfs:member)
      val rdfs12 = typeTriples
        .filter(t => t.objectMatches(RDFS.ContainerMembershipProperty.asNode()))
        .map(t => Triple.create(t.getSubject, RDF.`type`.asNode(), RDFS.member.asNode()))
        .name("rdfs12")

      // rdfs6: (p rdf:type rdf:Property) => (p rdfs:subPropertyOf p)
      val rdfs6 = typeTriples
        .filter(t => t.objectMatches(RDF.Property.asNode()))
        .map(t => Triple.create(t.getSubject, RDFS.subPropertyOf.asNode(), t.getSubject))
        .name("rdfs6")

      // rdfs8: (s rdf:type rdfs:Class ) => (s rdfs:subClassOf rdfs:Resource)
      // rdfs10: (s rdf:type rdfs:Class) => (s rdfs:subClassOf s)
      val rdfs8_10 = typeTriples
        .filter(t => t.objectMatches(RDFS.Class.asNode()))
        .flatMap(
          t =>
            Set(
              Triple.create(t.getSubject, RDFS.subClassOf.asNode(), RDFS.Resource.asNode()),
              Triple.create(t.getSubject, RDFS.subClassOf.asNode(), t.getSubject)
          )
        )
        .name("rdfs8/rdfs10")

      val additionalTripleRDDs = mutable.Seq(rdfs4, rdfs6, rdfs8_10, rdfs12)

      allTriples = env.union(Seq(allTriples) ++ additionalTripleRDDs).distinct(_.hashCode)
    }

    logger.info(
      "...finished materialization in " + (System
        .currentTimeMillis() - startTime) + "ms."
    )
//    val newSize = allTriples.count()
//    logger.info(s"|G_inf|=$newSize")

    // return graph with inferred triples
    RDFGraph(allTriples)
  }

  object SchemaTriplesFilter extends (Triple => Boolean) with Serializable {

    private val schemaPredicates =
      Set(RDFS.subClassOf, RDFS.subPropertyOf, RDFS.domain, RDFS.range).map(_.asNode())

    override def apply(t: Triple): Boolean = schemaPredicates.contains(t.getPredicate)
  }

  private def extractSchemaTriples(triples: DataSet[Triple]): DataSet[Triple] = {
    triples.filter(SchemaTriplesFilter).name("schemaTriples")
  }

  class SubClassOfFilterFunction(predicate: String) extends RichFilterFunction[Triple]() {

    var broadcastSet: Traversable[Triple] = _

    override def open(config: Configuration): Unit = {
      // Access the broadcasted DataSet as a Collection
      broadcastSet = getRuntimeContext.getBroadcastVariable[Triple](predicate).asScala
    }

    override def filter(t: Triple): Boolean =
      broadcastSet.exists(_.subjectMatches(t.getObject))
  }

  class SubClassOfFlatMapFunction(predicate: String) extends RichFlatMapFunction[Triple, Triple]() {
    var broadcastSet: Traversable[Triple] = _

    override def open(config: Configuration): Unit = {
      // Access the broadcasted DataSet as a Collection
      broadcastSet = getRuntimeContext.getBroadcastVariable[Triple](predicate).asScala
    }

    override def flatMap(in: Triple, collector: Collector[Triple]): Unit = {
      broadcastSet
        .filter(t => in.objectMatches(t.getSubject))
        .foreach(t => collector.collect(Triple.create(in.getSubject, in.getPredicate, t.getObject)))
    }
  }

}
