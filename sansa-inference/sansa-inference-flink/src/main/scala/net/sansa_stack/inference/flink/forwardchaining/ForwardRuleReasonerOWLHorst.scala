package net.sansa_stack.inference.flink.forwardchaining

import org.apache.flink.api.common.functions.{GroupReduceFunction, JoinFunction, RichFilterFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.vocabulary.{OWL2, RDF, RDFS}
import org.slf4j.LoggerFactory

import net.sansa_stack.inference.flink.data.RDFGraph
import net.sansa_stack.inference.utils.CollectionUtils
import java.lang.Iterable

import scala.jdk.CollectionConverters._

import org.apache.flink.configuration.Configuration

/**
  * A forward chaining implementation of the OWL Horst entailment regime.
  *
  * @constructor create a new OWL Horst forward chaining reasoner
  * @param env the Apache Flink execution environment
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOWLHorst(env: ExecutionEnvironment) extends ForwardRuleReasoner{

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  val tripleKeyFct : Triple => Int = {t => t.hashCode()}
  val nodePairKeyFct : ((Node, Node)) => Int = {case (n1, n2) => n1.hashCode() * 17 + n2.hashCode() * 31}
  val nodePairKVKeyFct : (((Node, Node), Node)) => Int = {case (k, v) => k._1.hashCode() * 17 + k._2.hashCode() * 31}
  val nodePairKVKeyFct2 : (((Node, Node), Nil.type )) => Int = {case (k, v) => k._1.hashCode() * 17 + k._2.hashCode() * 31}
  val fct1: (((Node, Node), Node), ((Node, Node), Nil.type)) => (Node, Node) = {case (l: ((Node, Node), Node), r: ((Node, Node), Nil.type)) => (l._2, r._1._1)}
  val fct2: (((Node, Node), Node), ((Node, Node), Node)) => (Node, Node) = {case (l: ((Node, Node), Node), r: ((Node, Node), Node)) => (l._2, r._1._1)}

  def apply(graph: RDFGraph): RDFGraph = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    // all the triples in the graph
    val triplesDS = graph.triples

    val schemaPredicates = Set(
      RDFS.subClassOf, RDFS.subPropertyOf, RDFS.domain, RDFS.range,
      OWL2.equivalentClass, OWL2.equivalentProperty, OWL2.allValuesFrom, OWL2.someValuesFrom, OWL2.inverseOf, OWL2.onProperty,
      OWL2.hasValue, OWL2.cardinality, OWL2.minCardinality, OWL2.maxCardinality, OWL2.maxQualifiedCardinality,
      OWL2.complementOf, OWL2.unionOf, OWL2.intersectionOf
    ).map(_.asNode())

    // split the triples into schema and instance data
    // (use a closure variable, but could also used broadcast variable here)
//    val schemaPredicatesDS = env.fromCollection(schemaPredicates)
//    val BC = "schemaPredicates"
    val schemaTriplesDS = triplesDS
      .filter(t => schemaPredicates.contains(t.getPredicate))
//        .filter(new RichFilterFunction[Triple] {
//      var schemaPredicates: Set[Node] = _
//      override def open(config: Configuration): Unit = {
//        schemaPredicates = getRuntimeContext.getBroadcastVariable[Node](BC).asScala.toSet
//      }
//      override def filter(t: Triple): Boolean = schemaPredicates.contains(t.getPredicate)
//    })
//      .withBroadcastSet(schemaPredicatesDS, BC)
      .name("schema triples")

    val instanceTriplesDS = triplesDS
      .filter(t => !schemaPredicates.contains(t.getPredicate))
//      .filter(new RichFilterFunction[Triple] {
//        var schemaPredicates: Set[Node] = _
//        override def open(config: Configuration): Unit = {
//          schemaPredicates = getRuntimeContext.getBroadcastVariable[Node](BC).asScala.toSet
//        }
//        override def filter(t: Triple): Boolean = !schemaPredicates.contains(t.getPredicate)
//      })
//      .withBroadcastSet(schemaPredicatesDS, BC)
      .name("instance data triples")


    // extract the schema data
    var subClassOfTriplesDS = extractTriples(schemaTriplesDS, RDFS.subClassOf.asNode()) // rdfs:subClassOf
    var subPropertyOfTriplesDS = extractTriples(schemaTriplesDS, RDFS.subPropertyOf.asNode()) // rdfs:subPropertyOf
    val domainTriplesDS = extractTriples(schemaTriplesDS, RDFS.domain.asNode()) // rdfs:domain
    val rangeTriplesDS = extractTriples(schemaTriplesDS, RDFS.range.asNode()) // rdfs:range
    val equivClassTriplesDS = extractTriples(schemaTriplesDS, OWL2.equivalentClass.asNode()) // owl:equivalentClass
    val equivPropertyTriplesDS = extractTriples(schemaTriplesDS, OWL2.equivalentProperty.asNode()) // owl:equivalentProperty


    // 1. we have to process owl:equivalentClass and owl:equivalentProperty before computing the transitive closure
    // rdfp12a: (?C owl:equivalentClass ?D) -> (?C rdfs:subClassOf ?D )
    val tmp_12a = equivClassTriplesDS.map(t => Triple.create(t.getSubject, RDFS.subClassOf.asNode(), t.getObject))
    // rdfp12b: (?C owl:equivalentClass ?D) -> (?D rdfs:subClassOf ?C )
    val tmp_12b = equivClassTriplesDS.map(t => Triple.create(t.getObject, RDFS.subClassOf.asNode(), t.getSubject))
    subClassOfTriplesDS = env.union(Seq(subClassOfTriplesDS, tmp_12a, tmp_12b))
                            .distinct(tripleKeyFct)

    // rdfp13a: (?C owl:equivalentProperty ?D) -> (?C rdfs:subPropertyOf ?D )
    val tmp_13a = equivPropertyTriplesDS.map(t => Triple.create(t.getSubject, RDFS.subPropertyOf.asNode(), t.getObject))
    // rdfp13b: (?C owl:equivalentProperty ?D) -> (?D rdfs:subPropertyOf ?C )
    val tmp_13b = equivPropertyTriplesDS.map(t => Triple.create(t.getObject, RDFS.subPropertyOf.asNode(), t.getSubject))
    subPropertyOfTriplesDS = env.union(Seq(subPropertyOfTriplesDS, tmp_13a, tmp_13b))
                              .distinct(tripleKeyFct)

    // 2. we compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf
    // rdfs11:  (xxx rdfs:subClassOf yyy), (yyy rdfs:subClassOf zzz) -> (xxx rdfs:subClassOf zzz)
    val subClassOfTriplesTransDS = computeTransitiveClosure(subClassOfTriplesDS).name("TC subClassOf")

    // rdfs5: (xxx rdfs:subPropertyOf yyy), (yyy rdfs:subPropertyOf zzz) -> (xxx rdfs:subPropertyOf zzz)
    val subPropertyOfTriplesTransDS = computeTransitiveClosure(subPropertyOfTriplesDS).name("TC subPropertyOf")


    // we put all into maps which should be more efficient later on
    val subClassOfMap = CollectionUtils.toMultiMap(subClassOfTriplesTransDS.map(t => (t.getSubject, t.getObject)).collect)
    val subPropertyMap = CollectionUtils.toMultiMap(subPropertyOfTriplesTransDS.map(t => (t.getSubject, t.getObject)).collect)
    val domainMap = domainTriplesDS.map(t => (t.getSubject, t.getObject)).collect.toMap
    val rangeMap = rangeTriplesDS.map(t => (t.getSubject, t.getObject)).collect.toMap

    // TODO broadcast schema in with Flink
//    // distribute the schema data structures by means of shared variables
//    // the assumption here is that the schema is usually much smaller than the instance data
//    val subClassOfMapBC = sc.broadcast(subClassOfMap)
//    val subPropertyMapBC = sc.broadcast(subPropertyMap)
//    val domainMapBC = sc.broadcast(domainMap)
//    val rangeMapBC = sc.broadcast(rangeMap)

    // compute the equivalence of classes and properties
    // rdfp12c: (?C rdfs:subClassOf ?D ), (?D rdfs:subClassOf ?C ) -> (?C owl:equivalentClass ?D)
    val equivClassTriplesInf = equivClassTriplesDS.union(
      subClassOfTriplesTransDS
        .filter(t => subClassOfMap.getOrElse(t.getObject, Set.empty).contains(t.getSubject))
        .map(t => Triple.create(t.getSubject, OWL2.equivalentClass.asNode(), t.getObject))
    ).name("rdfp12c")

    // rdfp13c: (?C rdfs:subPropertyOf ?D ), (?D rdfs:subPropertyOf ?C ) -> (?C owl:equivalentProperty ?D)
    val equivPropTriplesInf = equivPropertyTriplesDS.union(
      subPropertyOfTriplesTransDS
        .filter(t => subPropertyMap.getOrElse(t.getObject, Set.empty).contains(t.getSubject))
        .map(t => Triple.create(t.getSubject, OWL2.equivalentProperty.asNode(), t.getObject))
    ).name("rdfp13c")

    // we also extract properties with certain OWL characteristic and share them
    val transitiveProperties =
      extractTriples(triplesDS, None, None, Some(OWL2.TransitiveProperty.asNode())).name("transitive property triples")
        .map(triple => triple.getSubject)
        .collect()
    val functionalProperties =
      extractTriples(triplesDS, None, None, Some(OWL2.FunctionalProperty.asNode())).name("functional property triples")
        .map(triple => triple.getSubject)
        .collect()
    val inverseFunctionalProperties =
      extractTriples(triplesDS, None, None, Some(OWL2.InverseFunctionalProperty.asNode())).name("inverse functional property triples")
        .map(triple => triple.getSubject)
        .collect()
    val symmetricProperties =
      extractTriples(triplesDS, None, None, Some(OWL2.SymmetricProperty.asNode())).name("symmetric property triples")
        .map(triple => triple.getSubject)
        .collect()

    // and inverse property definitions
    val inverseOfMap =
      extractTriples(schemaTriplesDS, OWL2.inverseOf.asNode())
        .map(triple => (triple.getSubject, triple.getObject))
        .collect()
        .toMap
    val inverseOfMapReverted = inverseOfMap.map(_.swap)

    // and more OWL vocabulary used in property restrictions
    // owl:someValuesFrom
    val someValuesFromMap =
      extractTriples(schemaTriplesDS, OWL2.someValuesFrom.asNode())
        .map(triple => (triple.getSubject, triple.getObject))
        .collect()
        .toMap
    val someValuesFromMapReversed = someValuesFromMap.map(_.swap)
    // owl:allValuesFrom
    val allValuesFromMap =
      extractTriples(schemaTriplesDS, OWL2.allValuesFrom.asNode())
        .map(triple => (triple.getSubject, triple.getObject))
        .collect()
        .toMap
    // owl:hasValue
    val hasValueMap =
      extractTriples(schemaTriplesDS, OWL2.hasValue.asNode())
        .map(triple => (triple.getSubject, triple.getObject))
        .collect()
        .toMap
    // owl:onProperty
    val onPropertyMap =
      extractTriples(triplesDS, OWL2.onProperty.asNode())
        .map(triple => (triple.getSubject, triple.getObject))
        .collect()
        .toMap
    val onPropertyMapReversed = onPropertyMap.groupBy(_._2).mapValues(_.keys).map(identity)


    // owl:sameAs is computed separately, thus, we split the data
    var triplesFiltered = instanceTriplesDS.filter(t => !t.predicateMatches(OWL2.sameAs.asNode())
      && !t.predicateMatches(RDF.`type`.asNode()))
    var sameAsTriples = extractTriples(instanceTriplesDS, OWL2.sameAs.asNode())
    var typeTriples = extractTriples(instanceTriplesDS, RDF.`type`.asNode())

//    println("input rdf:type triples:\n" + typeTriples.collect().mkString("\n"))

    var newDataInferred = true
    var iteration = 0

    while(newDataInferred) {
      iteration += 1
      logger.info(iteration + ". iteration...")

      // 2. SubPropertyOf inheritance according to rdfs7 is computed

      /*
        rdfs7 aaa rdfs:subPropertyOf bbb .
              xxx aaa yyy .                    => xxx bbb yyy .
       */
      val triplesRDFS7 =
        triplesFiltered
          .filter(t => subPropertyMap.contains(t.getPredicate))
          .flatMap(t => subPropertyMap(t.getPredicate).map(supProp => Triple.create(t.getSubject, supProp, t.getObject)))
          .name("rdfs7")

      // add the inferred triples to the existing triples
      val rdfs7Res = triplesRDFS7.union(triplesFiltered)

      // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

      /*
      rdfs2 aaa rdfs:domain xxx .
            yyy aaa zzz .           => yyy rdf:type xxx .
       */
      val triplesRDFS2 =
        rdfs7Res
          .filter(t => domainMap.contains(t.getPredicate))
          .map(t => Triple.create(t.getSubject, RDF.`type`.asNode(), domainMap(t.getPredicate)))
          .name("rdfs2")

      /*
     rdfs3 aaa rdfs:range xxx .
           yyy aaa zzz .           => zzz rdf:type xxx .
      */
      val triplesRDFS3 =
        rdfs7Res
          .filter(t => rangeMap.contains(t.getPredicate))
          .map(t => Triple.create(t.getObject, RDF.`type`.asNode(), rangeMap(t.getPredicate)))
          .name("rdfs3")

      // 4. SubClass inheritance according to rdfs9
      // input are the rdf:type triples from RDFS2/RDFS3 and the ones contained in the original graph

      /*
      rdfs9 xxx rdfs:subClassOf yyy .
            zzz rdf:type xxx .         => zzz rdf:type yyy .
       */
      val triplesRDFS9 =
        triplesRDFS2
          .union(triplesRDFS3)
          .union(typeTriples)
          .filter(t => subClassOfMap.contains(t.getObject)) // such that A has a super class B
          .flatMap(t => subClassOfMap(t.getObject).map(supCls => Triple.create(t.getSubject, RDF.`type`.asNode(), supCls))) // create triple (s a B)
          .name("rdfs9")

      // rdfp14b: (?R owl:hasValue ?V),(?R owl:onProperty ?P),(?X rdf:type ?R ) -> (?X ?P ?V )
      val rdfp14b = typeTriples
          .flatMap {(t, out: Collector[Triple]) =>
            if (hasValueMap.contains(t.getObject) && onPropertyMap.contains(t.getObject)) {
              out.collect(Triple.create(t.getSubject, onPropertyMap(t.getObject), hasValueMap(t.getObject)))
            }
          }
//        .filter(triple =>
//          hasValueMap.contains(triple.getObject) &&
//          onPropertyMap.contains(triple.getObject)
//        )
//        .map(triple =>
//          Triple.create(triple.getSubject, onPropertyMap(triple.getObject), hasValueMap(triple.getObject))
//        )
        .name("rdfp14b")
      logger.whenDebugEnabled {
        println("rdfs14b:\n" + rdfp14b.collect().mkString("\n"))
      }

      // rdfp14a: (?R owl:hasValue ?V), (?R owl:onProperty ?P), (?U ?P ?V) -> (?U rdf:type ?R)
//      println(rdfs7Res.collect().mkString("\n"))
      val rdfp14a = rdfs7Res
        .filter(triple => {
          var valueRestrictionExists = false
          if (onPropertyMapReversed.contains(triple.getPredicate)) {
            // there is any restriction R for property P

            onPropertyMapReversed(triple.getPredicate).foreach { restriction =>
              if (hasValueMap.contains(restriction) && // R a hasValue restriction
                hasValueMap(restriction) == triple.getObject) {
                //  with value V
                valueRestrictionExists = true
              }
            }
          }
          valueRestrictionExists
        })
        .map(triple => {

          val s = triple.getSubject
          val p = RDF.`type`.asNode()
          var o: Node = null
          onPropertyMapReversed(triple.getPredicate).foreach { restriction => // get the restriction R
            if (hasValueMap.contains(restriction) && // R a hasValue restriction
              hasValueMap(restriction) == triple.getObject) { //  with value V

              o = restriction
            }

          }
          Triple.create(s, p, o)
        }
        )
        .name("rdfp14a")
      logger.whenDebugEnabled {
        println("rdf14a:\n" + rdfp14a.collect().mkString("\n"))
      }


      // rdfp8a: (?P owl:inverseOf ?Q), (?X ?P ?Y) -> (?Y ?Q ?X)
      val rdfp8a = triplesFiltered
        .filter(triple => inverseOfMap.contains(triple.getPredicate))
        .map(triple => Triple.create(triple.getObject, inverseOfMap(triple.getPredicate), triple.getSubject))
        .name("rdfp8a")

      // rdfp8b: (?P owl:inverseOf ?Q), (?X ?Q ?Y) -> (?Y ?P ?X)
      val rdfp8b = triplesFiltered
        .filter(triple => inverseOfMapReverted.contains(triple.getPredicate))
        .map(triple => Triple.create(triple.getObject, inverseOfMapReverted(triple.getPredicate), triple.getSubject))
        .name("rdfp8b")

      // rdfp3: (?P rdf:type owl:SymmetricProperty), (?X ?P ?Y) -> (?Y ?P ?X)
      val rdfp3 = triplesFiltered
        .filter(triple => symmetricProperties.contains(triple.getPredicate))
        .map(triple => Triple.create(triple.getObject, triple.getPredicate, triple.getSubject))
        .name("rdfp3")

      import org.apache.flink.api.scala._

      // rdfp15: (?R owl:someValuesFrom ?D), (?R owl:onProperty ?P), (?X ?P ?A), (?A rdf:type ?D ) -> (?X rdf:type ?R )
      val rdfp15_1 = triplesFiltered
        .filter(triple => onPropertyMapReversed.contains(triple.getPredicate)) // && someValuesFromMapBC.value.contains(onPropertyMapReversedBC.value(triple.predicate)))
        .flatMap(triple => {
          val restrictions = onPropertyMapReversed(triple.getPredicate)
          restrictions.map(_r => (_r -> triple.getObject, triple.getSubject)) // -> ((?R, ?A), ?X)
         })
//        .flatMap(identity)

      implicit val typeInfo = TypeInformation.of(classOf[((Node, Node), Nil.type)])
      val rdfp15_2 = typeTriples
        .filter(triple => someValuesFromMapReversed.contains(triple.getObject))
        .map(triple => ((someValuesFromMapReversed(triple.getObject), triple.getSubject), Nil)) // -> ((?R, ?A), NIL)


//      val tmp1: DataSet[(Node, Node)] = rdfp15_1.join(rdfp15_2).where(nodePairKVKeyFct).equalTo(nodePairKVKeyFct2) {case (l1, r2) => (l1, r2)}

      implicit val keyInfo: TypeInformation[Int] = createTypeInformation[Int]


      val rdfp15 = rdfp15_1
        .join(rdfp15_2).where(nodePairKVKeyFct).equalTo(nodePairKVKeyFct2)
//      (
//        new JoinFunction[((Node, Node), Node), ((Node, Node), Nil.type), (Node, Node)] {
//          override def join(l: ((Node, Node), Node), r: ((Node, Node), Nil.type)): (Node, Node) = {
//            (l._2, r._1._1)
//          }
//        )
        .apply(fct1)// ((?R, ?A), ?X) x ((?R, ?A), NIL)
        .map(e => Triple.create(e._1, RDF.`type`.asNode(), e._2)) // -> (?X rdf:type ?R )
        .name("rdfp15")
//      println(rdfp15.collect().mkString("\n"))


      // rdfp16: (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P), (?X ?P ?Y), (?X rdf:type ?R ) -> (?Y rdf:type ?D )
      val rdfp16_1 = triplesFiltered // (?X ?P ?Y)
        .filter(triple => onPropertyMapReversed.contains(triple.getPredicate) &&
                          allValuesFromMap.keySet.intersect(onPropertyMapReversed(triple.getPredicate).toSet).nonEmpty) // (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P)
        .flatMap(triple => {
          val restrictions = onPropertyMapReversed(triple.getPredicate)
          restrictions.map(_r => (triple.getSubject -> _r, triple.getObject)) // -> ((?X, ?R), ?Y)
        })
        .name("rdfp16_1")
//        .flatMap(identity)
      logger.whenDebugEnabled {
        println("rdfp16_1:\n" + rdfp16_1.collect().mkString("\n"))
      }

      val rdfp16_2 = typeTriples // (?X rdf:type ?R )
        .filter(triple => allValuesFromMap.contains(triple.getObject) && onPropertyMap.contains(triple.getObject)) // (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P)
        .map(triple => ((triple.getSubject, triple.getObject), allValuesFromMap(triple.getObject))) // -> ((?X, ?R), ?D)
        .name("rdfp16_2")
      logger.whenDebugEnabled {
        println("rdfp16_2:\n" + rdfp16_2.collect().mkString("\n"))
      }

//      import org.apache.flink.api.scala._
      val rdfp16 = rdfp16_1.join(rdfp16_2).where(nodePairKVKeyFct).equalTo(nodePairKVKeyFct)(keyInfo) {// ((?X, ?R), ?Y) x ((?X, ?R), ?D)
//      (l: ((Node, Node), Node), r: ((Node, Node), Node)) => (l._2, r._2)} // -> (Y, D)
//      {case (l: ((Node, Node), Node), r: ((Node, Node), Node)) => (l._2, r._2)} // -> (Y, D)
        (l, r) => Triple.create(l._2, RDF.`type`.asNode(), r._2)} // -> (Y, D)
//        .map(e => Triple.create(e._1, RDF.`type`.asNode(), e._2)) // -> (?Y rdf:type ?D )
        .name("rdfp16")
      //      println(rdfp15.collect().mkString("\n"))

      // deduplicate
      import net.sansa_stack.inference.flink.utils.DataSetUtils._
      val instanceTriplesNew = env.union(Seq(triplesRDFS7, rdfp3, rdfp8a, rdfp8b, rdfp14b))
        .distinct(tripleKeyFct)
        .subtract(triplesFiltered, tripleKeyFct, tripleKeyFct)

      val instanceTriplesNewCnt = instanceTriplesNew.count
      logger.debug("new spo triples:" + instanceTriplesNewCnt)

      // transitivity rule has to be applied always in the first iteration or when new triples have been inferred
      if(iteration == 1 || instanceTriplesNewCnt > 0) {
        // add triples
        triplesFiltered = triplesFiltered.union(instanceTriplesNew)

        // rdfp4: (?P rdf:type owl:TransitiveProperty), (?X ?P ?Y), (?Y ?P ?Z) -> (?X ?P ?Z)
        val rdfp4 = computeTransitiveClosure(triplesFiltered.filter(t => transitiveProperties.contains(t.getPredicate)))
            .name("rdfp4")

        // add triples
        triplesFiltered = triplesFiltered.union(rdfp4)
      }

      // deduplicate the computed rdf:type triples and check if new triples have been computed
      val typeTriplesNew = env.union(Seq(triplesRDFS2, triplesRDFS3, triplesRDFS9, rdfp14a, rdfp15, rdfp16))
        .distinct(_.hashCode())
        .subtract(typeTriples, tripleKeyFct, tripleKeyFct)

      val typeTriplesNewCnt = typeTriplesNew.count
      logger.debug("new type triples:" + typeTriplesNewCnt)

      if(typeTriplesNewCnt > 0) {
        // add type triples
        typeTriples = typeTriples.union(typeTriplesNew)
      }

      newDataInferred = instanceTriplesNewCnt > 0 || typeTriplesNewCnt > 0
    }

    // compute the owl:sameAs triples
    // rdfp1 and rdfp2 could be run in parallel

    // rdfp1: (?P rdf:type owl:FunctionalProperty), (?A ?P ?B), notLiteral(?B), (?A ?P ?C), notLiteral(?C), notEqual(?B ?C) -> (?B owl:sameAs ?C)
    val rdfp1_1 = triplesFiltered
      .filter(triple => functionalProperties.contains(triple.getPredicate))
      .map(triple => (triple.getSubject, triple.getPredicate) -> triple.getObject) // -> ((?A, ?P), ?B)
//    println(rdfp1_1.collect().mkString("\n"))
//    println("Joined:" + rdfp1_1.join(rdfp1_1).collect().mkString("\n"))

    // apply a) self join or b) groupBy + reduce
    val rdfp1 = rdfp1_1.groupBy(nodePairKVKeyFct).reduceGroup(new GroupReduceFunction[((Node, Node), Node), Triple] {
      override def reduce(values: Iterable[((Node, Node), Node)], out: Collector[Triple]): Unit = {
        val nodes = values.asScala map { e => e._2 }
        val pairs = for (x <- nodes; y <- nodes; if x != y) yield (x, y)
        pairs.toIterator map {case(n1, n2) => Triple.create(n1, OWL2.sameAs.asNode(), n2)} foreach out.collect
      }
    })
      .name("rdfp1")
//    val rdfp1 = rdfp1_1.join(rdfp1_1).where(nodePairKVKeyFct).equalTo(nodePairKVKeyFct) ({// ((?A, ?P), ?B) x ((?A, ?P), ?C)
//          (l, r) => (l._2, r._2) // -> (?B, ?C)
//        })
//        .filter(e => e._1 != e._2) // notEqual(?B ?C)
//        .map(e => Triple.create(e._1, OWL2.sameAs.asNode(), e._2)) // -> (?B owl:sameAs ?C)

    // rdfp2: (?P rdf:type owl:InverseFunctionalProperty), (?A ?P ?B), (?C ?P ?B), notEqual(?A ?C) -> (?A owl:sameAs ?C)
    val rdfp2_1 = triplesFiltered
      .filter(triple => inverseFunctionalProperties.contains(triple.getPredicate))
      .map(triple => (triple.getObject, triple.getPredicate) -> triple.getSubject) // -> ((?B, ?P), ?A)
    val rdfp2 = rdfp2_1.groupBy(nodePairKVKeyFct).reduceGroup(new GroupReduceFunction[((Node, Node), Node), Triple] {
      override def reduce(values: Iterable[((Node, Node), Node)], out: Collector[Triple]): Unit = {
        val nodes = values.asScala map { e => e._2 }
        val pairs = for (x <- nodes; y <- nodes; if x != y) yield (x, y)
        pairs.toIterator map {case(n1, n2) => Triple.create(n1, OWL2.sameAs.asNode(), n2)} foreach out.collect
      }
    })
      .name("rdfp2")

//    val rdfp2 = .join(rdfp2_1).where(0).equalTo(0) ({// ((?B, ?P), ?A) x ((?B, ?P), ?C)
//          (l, r) => (l._2, r._2) // -> (?A, ?C)
//        })
//        .filter(e => e._1 != e._2) // notEqual(?A ?C)
//        .map(e => Triple.create(e._1, OWL2.sameAs.asNode(), e._2)) // -> (?A owl:sameAs ?C)

    triplesFiltered = triplesFiltered.union(rdfp1).union(rdfp2)

    logger.info("...finished materialization in " + (System.currentTimeMillis() - startTime) + "ms.")

    // TODO check for deduplication
    triplesFiltered = deduplicate(triplesFiltered)
    typeTriples = deduplicate(typeTriples)


    // combine all inferred triples
    val inferredTriples = env.union(
      Seq(
        triplesFiltered,
        typeTriples,
        subClassOfTriplesTransDS,
        subPropertyOfTriplesTransDS,
        equivClassTriplesInf,
        equivPropTriplesInf
      )
    )
    .name("inferred triples")

    // return graph with inferred triples
    RDFGraph(inferredTriples)
  }

  def deduplicate(triples: DataSet[Triple]): DataSet[Triple] = {
    triples.distinct(tripleKeyFct)
  }

  // rdfp15: (?R owl:someValuesFrom ?D), (?R owl:onProperty ?P), (?X ?P ?A), (?A rdf:type ?D ) -> (?X rdf:type ?R )
//  def rdfp15(triples: RDD[RDFTriple]) : RDD[RDFTriple] = {
//
//    val rdfp15_1 = triples // (?X ?P ?A)
//      .filter(triple => onPropertyMapReversedBC.value.contains(triple.predicate)) // (?X ?P ?A), (?R owl:onProperty ?P)
//      .map(triple => {
//      val restrictions = onPropertyMapReversedBC.value(triple.predicate)
//      restrictions.map(_r => ((_r -> triple.`object`), triple.subject))
//    })
//      .flatMap(identity) // -> ((?R, ?A), ?X)
//
//    val rdfp15_2 = typeTriples // (?A rdf:type ?D )
//      .filter(triple => someValuesFromMapReversedBC.value.contains(triple.`object`)) // (?A rdf:type ?D ), (?R owl:someValuesFrom ?D)
//      .map(triple => ((someValuesFromMapReversedBC.value(triple.`object`), triple.subject), Nil)) // -> ((?R, ?A), NIL)
//
//    val rdfp15 = rdfp15_1
//      .join(rdfp15_2)
//      .map(e => Triple.create(e._2._1, RDF.`type`.asNode(), e._1._1)) // -> (?X rdf:type ?R )
//
//    rdfp15
//  }
}
