package net.sansa_stack.inference.flink.forwardchaining

import net.sansa_stack.inference.flink.data.RDFGraph
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.jena.vocabulary.{OWL2, RDF, RDFS}
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.utils.CollectionUtils
import org.slf4j.LoggerFactory

/**
  * A forward chaining implementation of the OWL Horst entailment regime.
  *
  * @constructor create a new OWL Horst forward chaining reasoner
  * @param env the Apache Flink execution environment
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOWLHorst(env: ExecutionEnvironment) extends ForwardRuleReasoner{

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def apply(graph: RDFGraph): RDFGraph = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    val triplesRDD = graph.triples


    // extract the schema data
    var subClassOfTriples = extractTriples(triplesRDD, RDFS.subClassOf.getURI) // rdfs:subClassOf
    var subPropertyOfTriples = extractTriples(triplesRDD, RDFS.subPropertyOf.getURI) // rdfs:subPropertyOf
    val domainTriples = extractTriples(triplesRDD, RDFS.domain.getURI) // rdfs:domain
    val rangeTriples = extractTriples(triplesRDD, RDFS.range.getURI) // rdfs:range
    val equivClassTriples = extractTriples(triplesRDD, OWL2.equivalentClass.getURI) // owl:equivalentClass
    val equivPropertyTriples = extractTriples(triplesRDD, OWL2.equivalentProperty.getURI) // owl:equivalentProperty


    // 1. we have to process owl:equivalentClass and owl:equivalentProperty before computing the transitive closure
    // rdfp12a: (?C owl:equivalentClass ?D) -> (?C rdfs:subClassOf ?D )
    val tmp_12a = equivClassTriples.map(t => RDFTriple(t.s, RDFS.subClassOf.getURI, t.o))
    // rdfp12b: (?C owl:equivalentClass ?D) -> (?D rdfs:subClassOf ?C )
    val tmp_12b = equivClassTriples.map(t => RDFTriple(t.o, RDFS.subClassOf.getURI, t.s))
    subClassOfTriples = env.union(Seq(subClassOfTriples, tmp_12a, tmp_12b))
                            .distinct()

    // rdfp13a: (?C owl:equivalentProperty ?D) -> (?C rdfs:subPropertyOf ?D )
    val tmp_13a = equivPropertyTriples.map(t => RDFTriple(t.s, RDFS.subPropertyOf.getURI, t.o))
    // rdfp13b: (?C owl:equivalentProperty ?D) -> (?D rdfs:subPropertyOf ?C )
    val tmp_13b = equivPropertyTriples.map(t => RDFTriple(t.o, RDFS.subPropertyOf.getURI, t.s))
    subPropertyOfTriples = env.union(Seq(subPropertyOfTriples, tmp_13a, tmp_13b))
                              .distinct()

    // 2. we compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf
    // rdfs11: 	(xxx rdfs:subClassOf yyy), (yyy rdfs:subClassOf zzz) ->	(xxx rdfs:subClassOf zzz)
    val subClassOfTriplesTrans = computeTransitiveClosure(subClassOfTriples)
    // rdfs5:	(xxx rdfs:subPropertyOf yyy), (yyy rdfs:subPropertyOf zzz) -> (xxx rdfs:subPropertyOf zzz)
    val subPropertyOfTriplesTrans = computeTransitiveClosure(subPropertyOfTriples)


    // we put all into maps which should be more efficient later on
    val subClassOfMap = CollectionUtils.toMultiMap(subClassOfTriplesTrans.map(t => (t.s, t.o)).collect)
    val subPropertyMap = CollectionUtils.toMultiMap(subPropertyOfTriplesTrans.map(t => (t.s, t.o)).collect)
    val domainMap = domainTriples.map(t => (t.s, t.o)).collect.toMap
    val rangeMap = rangeTriples.map(t => (t.s, t.o)).collect.toMap

    // TODO broadcast schema in with Flink
//    // distribute the schema data structures by means of shared variables
//    // the assumption here is that the schema is usually much smaller than the instance data
//    val subClassOfMapBC = sc.broadcast(subClassOfMap)
//    val subPropertyMapBC = sc.broadcast(subPropertyMap)
//    val domainMapBC = sc.broadcast(domainMap)
//    val rangeMapBC = sc.broadcast(rangeMap)

    // compute the equivalence of classes and properties
    // rdfp12c: (?C rdfs:subClassOf ?D ), (?D rdfs:subClassOf ?C ) -> (?C owl:equivalentClass ?D)
    val equivClassTriplesInf = equivClassTriples.union(
      subClassOfTriplesTrans
        .filter(t => subClassOfMap.getOrElse(t.o, Set.empty).contains(t.s))
        .map(t => RDFTriple(t.s, OWL2.equivalentClass.getURI, t.o))
    )

    // rdfp13c: (?C rdfs:subPropertyOf ?D ), (?D rdfs:subPropertyOf ?C ) -> (?C owl:equivalentProperty ?D)
    val equivPropTriplesInf = equivPropertyTriples.union(
      subPropertyOfTriplesTrans
        .filter(t => subPropertyMap.getOrElse(t.o, Set.empty).contains(t.s))
        .map(t => RDFTriple(t.s, OWL2.equivalentProperty.getURI, t.o))
    )

    // we also extract properties with certain OWL characteristic and share them
    val transitiveProperties =
      extractTriples(triplesRDD, None, None, Some(OWL2.TransitiveProperty.getURI))
        .map(triple => triple.s)
        .collect()
    val functionalProperties =
      extractTriples(triplesRDD, None, None, Some(OWL2.FunctionalProperty.getURI))
        .map(triple => triple.s)
        .collect()
    val inverseFunctionalProperties =
      extractTriples(triplesRDD, None, None, Some(OWL2.InverseFunctionalProperty.getURI))
        .map(triple => triple.s)
        .collect()
    val symmetricProperties =
      extractTriples(triplesRDD, None, None, Some(OWL2.SymmetricProperty.getURI))
        .map(triple => triple.s)
        .collect()

    // and inverse property definitions
    val inverseOfMap =
      extractTriples(triplesRDD, None, Some(OWL2.inverseOf.getURI), None)
        .map(triple => (triple.s, triple.o))
        .collect()
        .toMap
    val inverseOfMapReverted = inverseOfMap.map(_.swap)

    // and more OWL vocabulary used in property restrictions
    // owl:someValuesFrom
    val someValuesFromMap =
      extractTriples(triplesRDD, None, Some(OWL2.someValuesFrom.getURI), None)
        .map(triple => (triple.s, triple.o))
        .collect()
        .toMap
    val someValuesFromMapReversed = someValuesFromMap.map(_.swap)
    // owl:allValuesFrom
    val allValuesFromMap =
      extractTriples(triplesRDD, None, Some(OWL2.allValuesFrom.getURI), None)
        .map(triple => (triple.s, triple.o))
        .collect()
        .toMap
    val allValuesFromMapReversed = allValuesFromMap.map(_.swap)
    // owl:hasValue
    val hasValueMap =
      extractTriples(triplesRDD, None, Some(OWL2.hasValue.getURI), None)
        .map(triple => (triple.s, triple.o))
        .collect()
        .toMap
    val hasValueMapReversed = hasValueMap.groupBy(_._2).mapValues(_.keys).map(identity)
    // owl:onProperty
    val onPropertyMap =
      extractTriples(triplesRDD, None, Some(OWL2.onProperty.getURI), None)
        .map(triple => (triple.s, triple.o))
        .collect()
        .toMap
    val onPropertyMapReversed = onPropertyMap.groupBy(_._2).mapValues(_.keys).map(identity)


    // owl:sameAs is computed separately, thus, we split the data
    var triplesFiltered = triplesRDD.filter(triple => triple.p != OWL2.sameAs.getURI && triple.p != RDF.`type`.getURI)
    var sameAsTriples = triplesRDD.filter(triple => triple.p == OWL2.sameAs.getURI)
    var typeTriples = triplesRDD.filter(triple => triple.p == RDF.`type`.getURI)

//    println("input rdf:type triples:\n" + typeTriples.collect().mkString("\n"))

    var newDataInferred = true
    var iteration = 0

    while(newDataInferred) {
      iteration += 1
      logger.info(iteration + ". iteration...")

      // 2. SubPropertyOf inheritance according to rdfs7 is computed

      /*
        rdfs7	aaa rdfs:subPropertyOf bbb .
              xxx aaa yyy .                   	xxx bbb yyy .
       */
      val triplesRDFS7 =
        triplesFiltered
          .filter(t => subPropertyMap.contains(t.p))
          .flatMap(t => subPropertyMap(t.p).map(supProp => RDFTriple(t.s, supProp, t.o)))

      // add the inferred triples to the existing triples
      val rdfs7Res = triplesRDFS7.union(triplesFiltered)

      // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

      /*
      rdfs2	aaa rdfs:domain xxx .
            yyy aaa zzz .	          yyy rdf:type xxx .
       */
      val triplesRDFS2 =
        rdfs7Res
          .filter(t => domainMap.contains(t.p))
          .map(t => RDFTriple(t.s, RDF.`type`.getURI, domainMap(t.p)))

      /*
     rdfs3	aaa rdfs:range xxx .
           yyy aaa zzz .	          zzz rdf:type xxx .
      */
      val triplesRDFS3 =
        rdfs7Res
          .filter(t => rangeMap.contains(t.p))
          .map(t => RDFTriple(t.o, RDF.`type`.getURI, rangeMap(t.p)))


      // 4. SubClass inheritance according to rdfs9
      // input are the rdf:type triples from RDFS2/RDFS3 and the ones contained in the original graph

      /*
      rdfs9	xxx rdfs:subClassOf yyy .
            zzz rdf:type xxx .	        zzz rdf:type yyy .
       */
      val triplesRDFS9 =
        triplesRDFS2
          .union(triplesRDFS3)
          .union(typeTriples)
          .filter(t => subClassOfMap.contains(t.o)) // such that A has a super class B
          .flatMap(t => subClassOfMap(t.o).map(supCls => RDFTriple(t.s, RDF.`type`.getURI, supCls))) // create triple (s a B)


      // rdfp14b: (?R owl:hasValue ?V),(?R owl:onProperty ?P),(?X rdf:type ?R ) -> (?X ?P ?V )
      val rdfp14b = typeTriples
        .filter(triple =>
          hasValueMap.contains(triple.o) &&
          onPropertyMap.contains(triple.o)
        )
        .map(triple =>
          RDFTriple(triple.s, onPropertyMap(triple.o), hasValueMap(triple.o))
        )

      // rdfp14a: (?R owl:hasValue ?V), (?R owl:onProperty ?P), (?U ?P ?V) -> (?U rdf:type ?R)
//      println(rdfs7Res.collect().mkString("\n"))
      val rdfp14a = rdfs7Res
        .filter(triple => {
          var valueRestrictionExists = false
          if (onPropertyMapReversed.contains(triple.p)) {
            // there is any restriction R for property P

            onPropertyMapReversed(triple.p).foreach { restriction =>
              if (hasValueMap.contains(restriction) && // R a hasValue restriction
                hasValueMap(restriction) == triple.o) {
                //  with value V
                valueRestrictionExists = true
              }
            }
          }
          valueRestrictionExists
        })
        .map(triple => {

          val s = triple.s
          val p = RDF.`type`.getURI
          var o = ""
          onPropertyMapReversed(triple.p).foreach { restriction => // get the restriction R
            if (hasValueMap.contains(restriction) && // R a hasValue restriction
              hasValueMap(restriction) == triple.o) { //  with value V

              o = restriction
            }

          }
          RDFTriple(s, p, o)
        }
        )
      println(rdfp14a.collect().mkString("\n"))

      // rdfp8a: (?P owl:inverseOf ?Q), (?X ?P ?Y) -> (?Y ?Q ?X)
      val rdfp8a = triplesFiltered
        .filter(triple => inverseOfMap.contains(triple.p))
        .map(triple => RDFTriple(triple.o, inverseOfMap(triple.p), triple.s))

      // rdfp8b: (?P owl:inverseOf ?Q), (?X ?Q ?Y) -> (?Y ?P ?X)
      val rdfp8b = triplesFiltered
        .filter(triple => inverseOfMapReverted.contains(triple.p))
        .map(triple => RDFTriple(triple.o, inverseOfMapReverted(triple.p), triple.s))

      // rdfp3: (?P rdf:type owl:SymmetricProperty), (?X ?P ?Y) -> (?Y ?P ?X)
      val rdfp3 = triplesFiltered
        .filter(triple => symmetricProperties.contains(triple.p))
        .map(triple => RDFTriple(triple.o, triple.p, triple.s))

      // rdfp15: (?R owl:someValuesFrom ?D), (?R owl:onProperty ?P), (?X ?P ?A), (?A rdf:type ?D ) -> (?X rdf:type ?R )
      val rdfp15_1 = triplesFiltered
        .filter(triple => onPropertyMapReversed.contains(triple.p)) // && someValuesFromMapBC.value.contains(onPropertyMapReversedBC.value(triple.predicate)))
        .flatMap(triple => {
          val restrictions = onPropertyMapReversed(triple.p)
          restrictions.map(_r => (_r -> triple.o, triple.s)) // -> ((?R, ?A), ?X)
         })
//        .flatMap(identity)

      val rdfp15_2 = typeTriples
        .filter(triple => someValuesFromMapReversed.contains(triple.o))
        .map(triple => ((someValuesFromMapReversed(triple.o), triple.s), "s")) // -> ((?R, ?A), NIL)


      val rdfp15 = rdfp15_1
        .join(rdfp15_2).where(0).equalTo(0)({// ((?R, ?A), ?X) x ((?R, ?A), NIL)
             (l, r) => (l._2, r._1._1)
        })
        .map(e => RDFTriple(e._1, RDF.`type`.getURI, e._2)) // -> (?X rdf:type ?R )
//      println(rdfp15.collect().mkString("\n"))


      // rdfp16: (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P), (?X ?P ?Y), (?X rdf:type ?R ) -> (?Y rdf:type ?D )
      val rdfp16_1 = triplesFiltered // (?X ?P ?Y)
        .filter(triple => onPropertyMapReversed.contains(triple.p) &&
                          allValuesFromMap.keySet.intersect(onPropertyMapReversed(triple.p).toSet).nonEmpty) // (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P)
        .flatMap(triple => {
          val restrictions = onPropertyMapReversed(triple.p)
          restrictions.map(_r => (triple.s -> _r, triple.o)) // -> ((?X, ?R), ?Y)
        })
//        .flatMap(identity)

//      println("rdfp16_1:\n" + rdfp16_1.collect().mkString("\n"))

      val rdfp16_2 = typeTriples // (?X rdf:type ?R )
        .filter(triple => allValuesFromMap.contains(triple.o) && onPropertyMap.contains(triple.o)) // (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P)
        .map(triple => ((triple.s, triple.o), allValuesFromMap(triple.o))) // -> ((?X, ?R), ?D)

//      println("rdfp16_2:\n" + rdfp16_2.collect().mkString("\n"))

      val rdfp16 = rdfp16_1
        .join(rdfp16_2).where(0).equalTo(0) ({// ((?X, ?R), ?Y) x ((?X, ?R), ?D)
        (l, r) => (l._2, r._2) // -> (Y, D)
      })
        .map(e => RDFTriple(e._1, RDF.`type`.getURI, e._2)) // -> (?Y rdf:type ?D )
      //      println(rdfp15.collect().mkString("\n"))

      // deduplicate
      val triplesNew = env.union(Seq(triplesRDFS7, rdfp3, rdfp8a, rdfp8b, rdfp14b))
        .distinct()
//        .subtract(triplesFiltered, parallelism) TODO subtract in Flink???

      val tripleNewCnt = triplesNew.count

      if(iteration == 1 || tripleNewCnt > 0) {
        // add triples
        triplesFiltered = triplesFiltered.union(triplesNew)

        // rdfp4: (?P rdf:type owl:TransitiveProperty), (?X ?P ?Y), (?Y ?P ?Z) -> (?X ?P ?Z)
        val rdfp4 = computeTransitiveClosure(triplesFiltered.filter(triple => transitiveProperties.contains(triple.p)))

        // add triples
        triplesFiltered = triplesFiltered.union(rdfp4)
      }

      // deduplicate the computed rdf:type triples and check if new triples have been computed
      val typeTriplesNew = env.union(Seq(triplesRDFS2, triplesRDFS3, triplesRDFS9, rdfp14a, rdfp15, rdfp16))
        .distinct()
//        .subtract(typeTriples, parallelism) TODO subtract in Flink???

      val typeTriplesNewCnt = typeTriplesNew.count

      if(typeTriplesNewCnt > 0) {
        // add type triples
        typeTriples = typeTriples.union(typeTriplesNew)
      }

      newDataInferred = typeTriplesNewCnt > 0 || typeTriplesNewCnt > 0
    }

    // compute the owl:sameAs triples
    // rdfp1 and rdfp2 could be run in parallel

    // rdfp1: (?P rdf:type owl:FunctionalProperty), (?A ?P ?B), notLiteral(?B), (?A ?P ?C), notLiteral(?C), notEqual(?B ?C) -> (?B owl:sameAs ?C)
    val rdfp1_1 = triplesFiltered
      .filter(triple => functionalProperties.contains(triple.p))
      .map(triple => (triple.s, triple.p) -> triple.o) // -> ((?A, ?P), ?B)
//    println(rdfp1_1.collect().mkString("\n"))
//    println("Joined:" + rdfp1_1.join(rdfp1_1).collect().mkString("\n"))
    // apply self join
    val rdfp1 = rdfp1_1
        .join(rdfp1_1).where(0).equalTo(0) ({// ((?A, ?P), ?B) x ((?A, ?P), ?C)
          (l, r) => (l._2, r._2) // -> (?B, ?C)
        })
        .filter(e => e._1 != e._2) // notEqual(?B ?C)
        .map(e => RDFTriple(e._1, OWL2.sameAs.getURI, e._2)) // -> (?B owl:sameAs ?C)

    // rdfp2: (?P rdf:type owl:InverseFunctionalProperty), (?A ?P ?B), (?C ?P ?B), notEqual(?A ?C) -> (?A owl:sameAs ?C)
    val rdfp2_1 = triplesFiltered
      .filter(triple => inverseFunctionalProperties.contains(triple.p))
      .map(triple => (triple.o, triple.p) -> triple.s) // -> ((?B, ?P), ?A)
    val rdfp2 = rdfp2_1
        .join(rdfp2_1).where(0).equalTo(0) ({// ((?B, ?P), ?A) x ((?B, ?P), ?C)
          (l, r) => (l._2, r._2) // -> (?A, ?C)
        })
        .filter(e => e._1 != e._2) // notEqual(?A ?C)
        .map(e => RDFTriple(e._1, OWL2.sameAs.getURI, e._2)) // -> (?A owl:sameAs ?C)

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
        subClassOfTriplesTrans,
        subPropertyOfTriplesTrans,
        equivClassTriplesInf,
        equivPropTriplesInf
      )
    )

    // return graph with inferred triples
    RDFGraph(inferredTriples)
  }

  def deduplicate(triples: DataSet[RDFTriple]): DataSet[RDFTriple] = {
    triples.distinct()
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
//      .map(e => RDFTriple(e._2._1, RDF.`type`.getURI, e._1._1)) // -> (?X rdf:type ?R )
//
//    rdfp15
//  }
}
