package org.dissect.inference.forwardchaining

import org.apache.jena.vocabulary.{OWL2, RDF, RDFS}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.dissect.inference.data.{RDFGraph, RDFTriple}
import org.dissect.inference.utils.CollectionUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * A forward chaining implementation of the OWL Horst entailment regime.
  *
  * @constructor create a new OWL Horst forward chaining reasoner
  * @param sc the Apache Spark context
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerOWLHorst(sc: SparkContext, parallelism: Int) extends ForwardRuleReasoner{

  def this(sc: SparkContext) = this(sc, sc.defaultParallelism)

  private val logger = com.typesafe.scalalogging.slf4j.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def apply(graph: RDFGraph): RDFGraph = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    val triplesRDD = graph.triples.cache() // we cache this RDD because it's used quite often


    // extract the schema data
    var subClassOfTriples = extractTriples(triplesRDD, RDFS.subClassOf.getURI) // rdfs:subClassOf
    var subPropertyOfTriples = extractTriples(triplesRDD, RDFS.subPropertyOf.getURI) // rdfs:subPropertyOf
    val domainTriples = extractTriples(triplesRDD, RDFS.domain.getURI) // rdfs:domain
    val rangeTriples = extractTriples(triplesRDD, RDFS.range.getURI) // rdfs:range
    val equivClassTriples = extractTriples(triplesRDD, OWL2.equivalentClass.getURI) // owl:equivalentClass
    val equivPropertyTriples = extractTriples(triplesRDD, OWL2.equivalentProperty.getURI) // owl:equivalentProperty


    // 1. we have to process owl:equivalentClass and owl:equivalentProperty before computing the transitive closure
    // rdfp12a: (?C owl:equivalentClass ?D) -> (?C rdfs:subClassOf ?D )
    // rdfp12b: (?C owl:equivalentClass ?D) -> (?D rdfs:subClassOf ?C )
    subClassOfTriples = sc.union(
      subClassOfTriples,
      equivClassTriples.map(t => RDFTriple(t.subject, RDFS.subClassOf.getURI, t.`object`)),
      equivClassTriples.map(t => RDFTriple(t.`object`, RDFS.subClassOf.getURI, t.subject)))
      .distinct()

    // rdfp13a: (?C owl:equivalentProperty ?D) -> (?C rdfs:subPropertyOf ?D )
    // rdfp13b: (?C owl:equivalentProperty ?D) -> (?D rdfs:subPropertyOf ?C )
    subPropertyOfTriples = sc.union(
      subPropertyOfTriples,
      equivPropertyTriples.map(t => RDFTriple(t.subject, RDFS.subPropertyOf.getURI, t.`object`)),
      equivPropertyTriples.map(t => RDFTriple(t.`object`, RDFS.subPropertyOf.getURI, t.subject)))
      .distinct()

    // 2. we compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf
    // rdfs11: 	(xxx rdfs:subClassOf yyy), (yyy rdfs:subClassOf zzz) ->	(xxx rdfs:subClassOf zzz)
    val subClassOfTriplesTrans = computeTransitiveClosure(subClassOfTriples)

    /*
        rdfs5	xxx rdfs:subPropertyOf yyy .
              yyy rdfs:subPropertyOf zzz .	xxx rdfs:subPropertyOf zzz .
     */

    val subPropertyOfTriplesTrans = computeTransitiveClosure(subPropertyOfTriples)


    // we put all into maps which should be more efficient later on
    val subClassOfMap = CollectionUtils.toMultiMap(subClassOfTriplesTrans.map(t => (t.subject, t.`object`)).collect)
    val subPropertyMap = CollectionUtils.toMultiMap(subPropertyOfTriplesTrans.map(t => (t.subject, t.`object`)).collect)
    val domainMap = domainTriples.map(t => (t.subject, t.`object`)).collect.toMap
    val rangeMap = rangeTriples.map(t => (t.subject, t.`object`)).collect.toMap

    // distribute the schema data structures by means of shared variables
    // the assumption here is that the schema is usually much smaller than the instance data
    val subClassOfMapBC = sc.broadcast(subClassOfMap)
    val subPropertyMapBC = sc.broadcast(subPropertyMap)
    val domainMapBC = sc.broadcast(domainMap)
    val rangeMapBC = sc.broadcast(rangeMap)

    // compute the equivalence of classes and properties
    // rdfp12c: (?C rdfs:subClassOf ?D ), (?D rdfs:subClassOf ?C ) -> (?C owl:equivalentClass ?D)
    val equivClassTriplesInf = equivClassTriples.union(
      subClassOfTriplesTrans
        .filter(t => subClassOfMapBC.value.getOrElse(t.`object`, Set.empty).contains(t.subject))
        .map(t => RDFTriple(t.subject, OWL2.equivalentClass.getURI, t.`object`))
    )

    // rdfp13c: (?C rdfs:subPropertyOf ?D ), (?D rdfs:subPropertyOf ?C ) -> (?C owl:equivalentProperty ?D)
    val equivPropTriplesInf = equivPropertyTriples.union(
      subPropertyOfTriplesTrans
        .filter(t => subPropertyMapBC.value.getOrElse(t.`object`, Set.empty).contains(t.subject))
        .map(t => RDFTriple(t.subject, OWL2.equivalentProperty.getURI, t.`object`))
    )

    // we also extract properties with certain OWL characteristic and share them
    val transitivePropertiesBC = sc.broadcast(
      extractTriples(triplesRDD, None, None, Some(OWL2.TransitiveProperty.getURI))
        .map(triple => triple.subject)
        .collect())
    val functionalPropertiesBC = sc.broadcast(
      extractTriples(triplesRDD, None, None, Some(OWL2.FunctionalProperty.getURI))
        .map(triple => triple.subject)
        .collect())
    val inverseFunctionalPropertiesBC = sc.broadcast(
      extractTriples(triplesRDD, None, None, Some(OWL2.InverseFunctionalProperty.getURI))
        .map(triple => triple.subject)
        .collect())
    val symmetricPropertiesBC = sc.broadcast(
      extractTriples(triplesRDD, None, None, Some(OWL2.SymmetricProperty.getURI))
        .map(triple => triple.subject)
        .collect())

    // and inverse property definitions
    val inverseOfMapBC = sc.broadcast(
      extractTriples(triplesRDD, None, Some(OWL2.inverseOf.getURI), None)
        .map(triple => (triple.subject, triple.`object`))
        .collect()
        .toMap
    )
    val inverseOfMapRevertedBC = sc.broadcast(
      inverseOfMapBC.value.map(_.swap)
    )

    // and more OWL vocabulary used in property restrictions
    val someValuesFromMapBC = sc.broadcast(
      extractTriples(triplesRDD, None, Some(OWL2.someValuesFrom.getURI), None)
        .map(triple => (triple.subject, triple.`object`))
        .collect()
        .toMap
    )
    val someValuesFromMapReversedBC = sc.broadcast(
      someValuesFromMapBC.value.map(_.swap)
    )
    val allValuesFromMapBC = sc.broadcast(
      extractTriples(triplesRDD, None, Some(OWL2.allValuesFrom.getURI), None)
        .map(triple => (triple.subject, triple.`object`))
        .collect()
        .toMap
    )
    val allValuesFromMapReversedBC = sc.broadcast(
      allValuesFromMapBC.value.map(_.swap)
    )
    val hasValueMapBC = sc.broadcast(
      extractTriples(triplesRDD, None, Some(OWL2.hasValue.getURI), None)
        .map(triple => (triple.subject, triple.`object`))
        .collect()
        .toMap
    )
    val onPropertyMapBC = sc.broadcast(
      extractTriples(triplesRDD, None, Some(OWL2.onProperty.getURI), None)
        .map(triple => (triple.subject, triple.`object`))
        .collect()
        .toMap
    )
    val onPropertyMapReversedBC = sc.broadcast(
      onPropertyMapBC.value.groupBy(_._2).mapValues(_.keys).map(identity)
    )
    val hasValueMapReversedBC = sc.broadcast(
      hasValueMapBC.value.groupBy(_._2).mapValues(_.keys).map(identity)
    )


    // owl:sameAs is computed separately, thus, we split the data
    var triplesFiltered = triplesRDD.filter(triple => triple.predicate != OWL2.sameAs.getURI && triple.predicate != RDF.`type`.getURI)
    var sameAsTriples = triplesRDD.filter(triple => triple.predicate == OWL2.sameAs.getURI)
    var typeTriples = triplesRDD.filter(triple => triple.predicate == RDF.`type`.getURI)

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
          .filter(t => subPropertyMapBC.value.contains(t.predicate))
          .flatMap(t => subPropertyMapBC.value(t.predicate).map(supProp => RDFTriple(t.subject, supProp, t.`object`)))

      // add the inferred triples to the existing triples
      val rdfs7Res = triplesRDFS7.union(triplesFiltered)

      // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

      /*
      rdfs2	aaa rdfs:domain xxx .
            yyy aaa zzz .	          yyy rdf:type xxx .
       */
      val triplesRDFS2 =
        rdfs7Res
          .filter(t => domainMapBC.value.contains(t.predicate))
          .map(t => RDFTriple(t.subject, RDF.`type`.getURI, domainMapBC.value(t.predicate)))

      /*
     rdfs3	aaa rdfs:range xxx .
           yyy aaa zzz .	          zzz rdf:type xxx .
      */
      val triplesRDFS3 =
        rdfs7Res
          .filter(t => rangeMapBC.value.contains(t.predicate))
          .map(t => RDFTriple(t.`object`, RDF.`type`.getURI, rangeMapBC.value(t.predicate)))


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
          .filter(t => subClassOfMapBC.value.contains(t.`object`)) // such that A has a super class B
          .flatMap(t => subClassOfMapBC.value(t.`object`).map(supCls => RDFTriple(t.subject, RDF.`type`.getURI, supCls))) // create triple (s a B)


      // rdfp14b: (?R owl:hasValue ?V),(?R owl:onProperty ?P),(?X rdf:type ?R ) -> (?X ?P ?V )
      val rdfp14b = typeTriples
        .filter(triple =>
          hasValueMapBC.value.contains(triple.`object`) &&
          onPropertyMapBC.value.contains(triple.`object`)
        )
        .map(triple =>
          RDFTriple(triple.subject, onPropertyMapBC.value(triple.`object`), hasValueMapBC.value(triple.`object`))
        )

      // rdfp14a: (?R owl:hasValue ?V), (?R owl:onProperty ?P), (?U ?P ?V) -> (?U rdf:type ?R)
//      println(rdfs7Res.collect().mkString("\n"))
      val rdfp14a = rdfs7Res
        .filter(triple => {
          var valueRestrictionExists = false
          if (onPropertyMapReversedBC.value.contains(triple.predicate)) {
            // there is any restriction R for property P

            onPropertyMapReversedBC.value(triple.predicate).foreach { restriction =>
              if (hasValueMapBC.value.contains(restriction) && // R a hasValue restriction
                hasValueMapBC.value(restriction) == triple.`object`) {
                //  with value V
                valueRestrictionExists = true
              }
            }
          }
          valueRestrictionExists
        })
        .map(triple => {

          val s = triple.subject
          val p = RDF.`type`.getURI
          var o = ""
          onPropertyMapReversedBC.value(triple.predicate).foreach { restriction => // get the restriction R
            if (hasValueMapBC.value.contains(restriction) && // R a hasValue restriction
              hasValueMapBC.value(restriction) == triple.`object`) { //  with value V

              o = restriction
            }

          }
          RDFTriple(s, p, o)
        }
        )
      println(rdfp14a.collect().mkString("\n"))

      // rdfp8a: (?P owl:inverseOf ?Q), (?X ?P ?Y) -> (?Y ?Q ?X)
      val rdfp8a = triplesFiltered
        .filter(triple => inverseOfMapBC.value.contains(triple.predicate))
        .map(triple => RDFTriple(triple.`object`, inverseOfMapBC.value(triple.predicate), triple.subject))

      // rdfp8b: (?P owl:inverseOf ?Q), (?X ?Q ?Y) -> (?Y ?P ?X)
      val rdfp8b = triplesFiltered
        .filter(triple => inverseOfMapRevertedBC.value.contains(triple.predicate))
        .map(triple => RDFTriple(triple.`object`, inverseOfMapRevertedBC.value(triple.predicate), triple.subject))

      // rdfp3: (?P rdf:type owl:SymmetricProperty), (?X ?P ?Y) -> (?Y ?P ?X)
      val rdfp3 = triplesFiltered
        .filter(triple => symmetricPropertiesBC.value.contains(triple.predicate))
        .map(triple => RDFTriple(triple.`object`, triple.predicate, triple.subject))

      // rdfp15: (?R owl:someValuesFrom ?D), (?R owl:onProperty ?P), (?X ?P ?A), (?A rdf:type ?D ) -> (?X rdf:type ?R )
      val rdfp15_1 = triplesFiltered
        .filter(triple => onPropertyMapReversedBC.value.contains(triple.predicate)) // && someValuesFromMapBC.value.contains(onPropertyMapReversedBC.value(triple.predicate)))
        .map(triple => {
          val restrictions = onPropertyMapReversedBC.value(triple.predicate)
          restrictions.map(_r => (_r -> triple.`object`, triple.subject)) // -> ((?R, ?A), ?X)
         })
        .flatMap(identity)

      val rdfp15_2 = typeTriples
        .filter(triple => someValuesFromMapReversedBC.value.contains(triple.`object`))
        .map(triple => ((someValuesFromMapReversedBC.value(triple.`object`), triple.subject), Nil)) // -> ((?R, ?A), NIL)

      val rdfp15 = rdfp15_1
        .join(rdfp15_2)
        .map(e => RDFTriple(e._2._1, RDF.`type`.getURI, e._1._1)) // -> (?X rdf:type ?R )

//      println(rdfp15.collect().mkString("\n"))


      // rdfp16: (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P), (?X ?P ?Y), (?X rdf:type ?R ) -> (?Y rdf:type ?D )
      val rdfp16_1 = triplesFiltered // (?X ?P ?Y)
        .filter(triple => onPropertyMapReversedBC.value.contains(triple.predicate) &&
                          allValuesFromMapBC.value.keySet.intersect(onPropertyMapReversedBC.value(triple.predicate).toSet).nonEmpty) // (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P)
        .map(triple => {
          val restrictions = onPropertyMapReversedBC.value(triple.predicate)
          restrictions.map(_r => (triple.subject -> _r, triple.`object`)) // -> ((?X, ?R), ?Y)
        })
        .flatMap(identity)

//      println("rdfp16_1:\n" + rdfp16_1.collect().mkString("\n"))

      val rdfp16_2 = typeTriples // (?X rdf:type ?R )
        .filter(triple => allValuesFromMapBC.value.contains(triple.`object`) && onPropertyMapBC.value.contains(triple.`object`)) // (?R owl:allValuesFrom ?D), (?R owl:onProperty ?P)
        .map(triple => ((triple.subject, triple.`object`), allValuesFromMapBC.value(triple.`object`))) // -> ((?X, ?R), ?D)

//      println("rdfp16_2:\n" + rdfp16_2.collect().mkString("\n"))

      val rdfp16 = rdfp16_1
        .join(rdfp16_2) // (?X, ?R), (?Y, ?D)
        .map(e => RDFTriple(e._2._1, RDF.`type`.getURI, e._2._2)) // -> (?Y rdf:type ?D )

      // deduplicate
      val triplesNew = new UnionRDD(sc, Seq(triplesRDFS7, rdfp3, rdfp8a, rdfp8b, rdfp14b))
        .distinct(parallelism)
        .subtract(triplesFiltered, parallelism)

      val tripleNewCnt = triplesNew.count

      if(iteration == 1 || tripleNewCnt > 0) {
        // add triples
        triplesFiltered = triplesFiltered.union(triplesNew)

        // rdfp4: (?P rdf:type owl:TransitiveProperty), (?X ?P ?Y), (?Y ?P ?Z) -> (?X ?P ?Z)
        val rdfp4 = computeTransitiveClosure(triplesFiltered.filter(triple => transitivePropertiesBC.value.contains(triple.predicate)))

        // add triples
        triplesFiltered = triplesFiltered.union(rdfp4)
      }

      // deduplicate the computed rdf:type triples and check if new triples have been computed
      val typeTriplesNew = new UnionRDD(sc, Seq(triplesRDFS2, triplesRDFS3, triplesRDFS9, rdfp14a, rdfp15, rdfp16))
        .distinct(parallelism)
        .subtract(typeTriples, parallelism)

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
      .filter(triple => functionalPropertiesBC.value.contains(triple.predicate))
      .map(triple => (triple.subject, triple.predicate) -> triple.`object`) // -> ((?A, ?P), ?B)
//    println(rdfp1_1.collect().mkString("\n"))
//    println("Joined:" + rdfp1_1.join(rdfp1_1).collect().mkString("\n"))
    // apply self join
    val rdfp1 = rdfp1_1
        .join(rdfp1_1) // -> (?A, ?P), (?B, ?C)
        .filter(e => e._2._1 != e._2._2) // notEqual(?B ?C)
        .map(e => RDFTriple(e._2._1, OWL2.sameAs.getURI, e._2._2)) // -> (?B owl:sameAs ?C)

    // rdfp2: (?P rdf:type owl:InverseFunctionalProperty), (?A ?P ?B), (?C ?P ?B), notEqual(?A ?C) -> (?A owl:sameAs ?C)
    val rdfp2_1 = triplesFiltered
      .filter(triple => inverseFunctionalPropertiesBC.value.contains(triple.predicate))
      .map(triple => (triple.`object`, triple.predicate) -> triple.subject) // -> ((?B, ?P), ?A)
    val rdfp2 = rdfp2_1
        .join(rdfp2_1) // -> (?B, ?P), (?A, ?C)
        .filter(e => e._2._1 != e._2._2) // notEqual(?A ?C)
        .map(e => RDFTriple(e._2._1, OWL2.sameAs.getURI, e._2._2)) // -> (?A owl:sameAs ?C)

    triplesFiltered = triplesFiltered.union(rdfp1).union(rdfp2)

    logger.info("...finished materialization in " + (System.currentTimeMillis() - startTime) + "ms.")

    // TODO check for deduplication
    triplesFiltered = deduplicate(triplesFiltered)
    typeTriples = deduplicate(typeTriples)


    // combine all inferred triples
    val inferredTriples = sc.union(
      triplesFiltered,
      typeTriples,
      subClassOfTriplesTrans,
      subPropertyOfTriplesTrans,
      equivClassTriplesInf,
      equivPropTriplesInf
     )

    // return graph with inferred triples
    RDFGraph(inferredTriples)
  }

  def deduplicate(triples: RDD[RDFTriple]): RDD[RDFTriple] = {
    triples.distinct(parallelism)
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
