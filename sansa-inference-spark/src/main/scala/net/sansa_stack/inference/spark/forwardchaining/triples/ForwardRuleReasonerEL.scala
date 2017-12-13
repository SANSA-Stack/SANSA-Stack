package net.sansa_stack.inference.spark.forwardchaining.triples

import net.sansa_stack.inference.spark.data.model.RDFGraph
import net.sansa_stack.inference.spark.utils.RDFSSchemaExtractor
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.vocabulary.{OWL2, RDF, RDFS}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
  * This rule-based forward chaining reasoner implementation is based on the
  * paper
  *
  *   'Pushing the EL envelope' by Baader, Brandt, Lutz. IJCAI. Vol. 5. 2005.
  *
  * The paper does not cover the whole OWL 2 EL profile and makes some
  * assumptions w.r.t. the allowed axioms. Given the set BC that contains
  *
  * - owl:Thing
  * - all concept names
  * - all nominals { a } (with just one individual!)
  * - concrete domain descriptions p(f1, ..., fk) (not used here)
  *
  * The allowed general concept inclusions are the following (with C1, C2 \in BC,
  * D \in BC \cup { owl:Nothin }):
  *
  * -           C1 \sqsubseteq D
  * - C1 \sqcap C2 \sqsubseteq D
  * -           C1 \sqsubseteq \exists r.C2
  * - \exists r.C1 \sqsubseteq D
  *
  * Role inclusions may be of the form
  *
  * -       r \sqsubseteq s , or
  * - r1 o r2 \sqsubseteq s
  *
  * We implemented the completion rules that were feasible in our setting.
  *
  * Naming conventions:
  * - C, C1, C2 --> class names, or nominals (with just one individual),
  *   or owl:Thing
  * - D, D1, D2 --> class names, or nominals (with just one individual),
  *   owl:Thing, or owl:Nothing
  *
  * @param sc The Spark context
  * @param parallelism The degreee of parallelism (mainly used to derive the
  *                    number of partitions for RDD.distinct(parallelism) calls)
  */
class ForwardRuleReasonerEL (sc: SparkContext, parallelism: Int = 2) extends TransitiveReasoner(sc, parallelism) {
  private val logger = com.typesafe.scalalogging.Logger(
    LoggerFactory.getLogger(this.getClass.getName))

  // ------------------ <rule definitions> ------------------------------------
  case class Rule(name: String, getInferredTriples: RDD[Triple] => RDD[Triple]) {
    private var influencedRules: List[Rule] = List.empty

    def execute(triples: RDD[Triple]): RDD[Triple] = {
      triples.union(getInferredTriples(triples).distinct(parallelism))
    }

    def getInfluencedRules(): List[Rule] = influencedRules

    def setInfluencedRules(rules: List[Rule]): Unit = {
      this.influencedRules = rules
    }

    override def toString: String = "Rule " + name
  }

  /**
    * CR1: C1 \sqsubseteq C2, C2 \sqsubseteq D => C1 \sqsubseteq D
    */
  val cr1 = Rule("CR1", triples => {
    logger.info("CR1 called")
    val subClsOfPairs: RDD[(Node, Node)] = extractAtomicSubClassOf(triples)
    val superClsOfPairs: RDD[(Node, Node)] = subClsOfPairs.map(p => (p._2, p._1))

    subClsOfPairs.join(superClsOfPairs)
      // (join class, (sup cls, sub cls)) => (sub cls rdfs:subClassOf sup cls)
      .map(e => Triple.create(e._2._2, RDFS.subClassOf.asNode(), e._2._1))
  })

  /**
    * CR2: C \sqsubseteq C1, C \sqsubseteq C2,
    *      C1 \sqcap C2 \sqsubseteq D          => C \sqsubseteq D
    */
  val cr2 = Rule("CR2", triples => {
    logger.info("CR2 called")
    val subClsOfPairs: RDD[(Node, Node)] = extractAtomicSubClassOf(triples)
    val clsPairsWithCommonSubCls: RDD[((Node, Node), Node)] =
      subClsOfPairs.join(subClsOfPairs).map(e => (e._2, e._1))

    val intersctSCORelations: RDD[((Node, Node), Node)] =
      extractIntersectionSCORelations(triples).map(e => ((e._1, e._2), e._3))

    clsPairsWithCommonSubCls.join(intersctSCORelations).map(e => e._2)
      .map(e => Triple.create(e._1, RDFS.subClassOf.asNode(), e._2))
  })

  /**
    * CR3: C1 \sqsubseteq C2, C2 \sqsubseteq \exists r.D => C1 \sqsubseteq \exists r.D
    */
  val cr3 = Rule("CR3", triples => {
    logger.info("CR3 called")
    //                       (C2,   C1)
    val superClsOfPairs: RDD[(Node, Node)] =
      extractAtomicSubClassOf(triples).map(p => (p._2, p._1))

    //                           (C2,   (r,    D))
    val scoExistentialPairs: RDD[(Node, (Node, Node))] =
      extractSCOExistentialRelations(triples).map(e => (e._1, (e._2, e._3)))

    superClsOfPairs.join(scoExistentialPairs)
      // (C2, (C1, (r, D))) => (C1, r, D)
      .map(e => (e._2._1, e._2._2._1, e._2._2._2))
      .flatMap(e => {
        val c1 = e._1
        val r = e._2
        val d = e._3
        // TODO: revise
        val hash = (c1.hashCode() * r.hashCode()) % d.hashCode()
        val bNode = NodeFactory.createBlankNode(hash.toString)

        // C1 rdf:subClassOf _:23 .
        val trpl1 = Triple.create(c1, RDFS.subClassOf.asNode(), bNode)
        // _:23 owl:onProperty r .
        val trpl2 = Triple.create(bNode, OWL2.onProperty.asNode(), r)
        // _:23 owl:someValuesFrom D .
        val trpl3 = Triple.create(bNode, OWL2.someValuesFrom.asNode(), d)

        Seq(trpl1, trpl2, trpl3)
      })
  })

  /**
    * CR4: C \sqsubseteq \exists r.D, D \sqsubseteq D2,
    *      \exists r.D2 \sqsubseteq E                    => C \sqsubseteq E
    */
  val cr4 = Rule("CR4", triples => {
    logger.info("CR4 called")
    //                         (C,    r,    D)
    val scoExistRelations: RDD[(Node, Node, Node)] =
      extractSCOExistentialRelations(triples)
    //                (D,    D2)
    val scoPairs: RDD[(Node, Node)] = extractAtomicSubClassOf(triples)
    //                          ((r,    D2),   E)
    val existsSCORelations: RDD[((Node, Node), Node)] =
      extractExistentialSCORelations(triples).map(e => ((e._1, e._2), e._3))

    // variable naming now refers to the letters in the pattern descriptions in
    // the comments
    val rD2C: RDD[((Node, Node), Node)] =
      //                         (D,    (C,    r))          (D, D2)
      scoExistRelations.map(e => (e._3, (e._1, e._2))).join(scoPairs)
        // (D, ((C, r), D2)) ==> ((r, D2), C)
        .map(e => ((e._2._1._2, e._2._2), e._2._1._1))

    //                        ((r, D2), (C, E)) => (C, E)
    rD2C.join(existsSCORelations).map(_._2)
      .map(e => Triple.create(e._1, RDFS.subClassOf.asNode(), e._2))
  })

  /**
    * CR5: C\sqsubseteq \exists r.D, D \sqsubseteq owl:Noting => C \sqsubseteq owl:Nothing
    */
  val cr5 = Rule("CR5", triples => {
    logger.info("CR5 called")
    //                        (C,    r,    D)
    val scoExistRelations: RDD[(Node, Node, Node)] = extractSCOExistentialRelations(triples)
    //              (D,    owl:Nothing)
    val scoBot: RDD[(Node, Node)] = triples.filter(trpl => {
      trpl.predicateMatches(RDFS.subClassOf.asNode()) && trpl.objectMatches(OWL2.Nothing.asNode())
    }).map(trpl => (trpl.getSubject, trpl.getObject))

    // variable naming now refers to the letters in the pattern descriptions in
    // the comments
    val dC = scoExistRelations.map(e => (e._3, e._1))

    //     (D, (C, owl:Nothing)) => (C rdfs:subClassOf owl:Nothing)
    dC.join(scoBot).map(e => Triple.create(e._2._1, RDFS.subClassOf.asNode(), e._2._2))
  })

  /**
    * CR10: C \sqsubseteq \exists r.D, r \sqsubseteq s  =>  C \sqsubseteq \exists s.D
    */
  val cr10 = Rule("CR10", triples => {
    logger.info("CR10 called")
    //                         (C,    r,    D
    val scoExistRelations: RDD[(Node, Node, Node)] = extractSCOExistentialRelations(triples)
    //                (r,    s)
    val spoPairs: RDD[(Node, Node)] =
      triples.filter(_.predicateMatches(RDFS.subPropertyOf.asNode()))
        .map(trpl => (trpl.getSubject, trpl.getObject))

    // variable naming now refers to the letters in the pattern descriptions in
    // the comments

    //           (r,    (C,    D)
    val rCD: RDD[(Node, (Node, Node))] = scoExistRelations.map(e => (e._2, (e._1, e._3)))

    // (r, (s, (C, D))) => C rdfs:subClassOf [ owl:onProperty s . owl:someValuesFrom D ]
    spoPairs.join(rCD).flatMap(e => {
      val s = e._2._1
      val c = e._2._2._1
      val d = e._2._2._2
      // TODO: Revise
      val hash = (s.hashCode() * c.hashCode()) % d.hashCode()
      val bNode = NodeFactory.createBlankNode(hash.toString)

      // C rdfs:subClassOf _:23 .
      val trpl1 = Triple.create(c, RDFS.subClassOf.asNode(), bNode)
      // _:23 owl:onProperty s .
      val trpl2 = Triple.create(bNode, OWL2.onProperty.asNode(), s)
      // _:23 owl:someValuesFrom D .
      val trpl3 = Triple.create(bNode, OWL2.someValuesFrom.asNode(), d)

      Seq(trpl1, trpl2, trpl3)
    })
  })

  /**
    * CR11: C \sqsubseteq \exists r1.D,
    *       D \sqsubseteq \exists r2.E,
    *       r1 o r2 \sqsubseteq r3       => C \sqsubseteq \exists r3.E
    */
  val cr11 = Rule("CR11", triples => {
    logger.info("CR11 called")
    //                         (C,    r1,   D)
    //                         (D,    r2,   E)
    val scoExistRelations: RDD[(Node, Node, Node)] =
      extractSCOExistentialRelations(triples)

    //                          ((r1,   r2),   r3)
    val roleChainRelations: RDD[((Node, Node), Node)] =
      extractPropertyChainRelations(triples).map(e => ((e._1, e._2), e._3))

    // variable naming now refers to the letters in the pattern descriptions in
    // the comments

    val dr1C: RDD[(Node, (Node, Node))] =
      scoExistRelations.map(e => (e._3, (e._2, e._1)))

    val dr2E: RDD[(Node, (Node, Node))] =
      scoExistRelations.map(e => (e._1, (e._2, e._3)))

    //              ((r1,   r2),   (C,    D)
    val r1r2CE: RDD[((Node, Node), (Node, Node))] = dr1C.join(dr2E)
      //  (D, ((r1, C), (r2, E)))  => ((r1, r2), (C, E))
      .map(e => ((e._2._1._1, e._2._2._1), (e._2._1._2, e._2._2._2)))

    //                   ((r1, r2), (r3, (C, E))) => (C, r3, E)
    roleChainRelations.join(r1r2CE)
      .flatMap(entry => {
        val c = entry._2._2._1
        val r3 = entry._2._1
        val e = entry._2._2._2
        // TODO: revise
        val hash = (c.hashCode() * r3.hashCode()) % e.hashCode()
        val bNode = NodeFactory.createBlankNode(hash.toString)
        // C \sqsubseteq \exists r2.E

        // C rdfs:subClassOf _:23 .
        val trpl1 = Triple.create(c, RDFS.subClassOf.asNode(), bNode)
        // _:23 owl:onProperty r3 .
        val trpl2 = Triple.create(bNode, OWL2.onProperty.asNode(), r3)
        // _:23 owl:someValuesFrom E .
        val trpl3 = Triple.create(bNode, OWL2.someValuesFrom.asNode(), e)

        Seq(trpl1, trpl2, trpl3)
      })
  })
  cr1.setInfluencedRules(List(cr1, cr2, cr3, cr4, cr5))
  cr2.setInfluencedRules(List(cr1, cr2, cr3, cr4, cr5))
  cr3.setInfluencedRules(List(cr3, cr4, cr5, cr10, cr11))
  cr4.setInfluencedRules(List(cr1, cr2, cr3, cr4, cr5))
  cr5.setInfluencedRules(List(cr1, cr5))
  cr10.setInfluencedRules(List(cr3, cr4, cr5, cr10, cr11))
  cr11.setInfluencedRules(List(cr3, cr4, cr5, cr10, cr11))

  // ------------------ </rule definitions> -----------------------------------

  override def apply(graph: RDFGraph): RDFGraph = {
    logger.info("Materializing graph...")

    val startTime = System.currentTimeMillis()

    var triplesRDD = graph.triples
    triplesRDD.cache()

    // TODO: optimze order!
    val rulesQueue: mutable.Queue[Rule] =
      mutable.Queue(cr1, cr2, cr3, cr4, cr5, cr10, cr11)

    var rule: Rule = null
    var inferredTriples: RDD[Triple] = null
    while (rulesQueue.nonEmpty) {
      rule = rulesQueue.dequeue()
      inferredTriples = rule.getInferredTriples(triplesRDD)
      logger.info("---- Inferred " + inferredTriples.count() + " triples")

      if (!inferredTriples.isEmpty()) {
        // Check whether something new was inferred
        val oldCount = triplesRDD.count()
        logger.debug("Old overall count: " + oldCount)
        triplesRDD = triplesRDD.union(inferredTriples).distinct(parallelism)
        val newCount = triplesRDD.count()
        logger.debug("New overall count: " + newCount)
        if (newCount > oldCount) {
          rule.getInfluencedRules().foreach(rulesQueue.enqueue(_))
        }
      }
    }

    RDFGraph(triplesRDD)
  }

  /**
    * Extracts subclass-of relations of atomic classes,
    * i.e. SubClass \sqsubseteq SuperClass .
    * These relations are returned as pairs (SubClass, SuperClass).
    *
    * @param triples Input Triple RDD
    * @return An RDD of pairs of the shape (SubClass, SuperClass)
    */
  private def extractAtomicSubClassOf(triples: RDD[Triple]): RDD[(Node, Node)] = {
    triples.filter(_.getPredicate == RDFS.subClassOf.asNode())
        .map(triple => (triple.getSubject, triple.getObject))
  }

  /**
    * Extracts all blank nodes that represent an existential restriction,
    * together with the corresponding property and filler class, i.e. all
    * class expressions of the form \exists property.FillerClass which are
    * represented as follows in RDF (ntriples serialization):
    *
    *   _:1 owl:onProperty :property .
    *   _:1 owl:someValuesFrom :FillerClass .
    *
    * The extistential restrictions are returned as triplets of the form
    * (_:1, property, FillerClass).
    *
    * @param triples Input Triple RDD
    * @return An RDD of triplets holding the blank node, the property and the
    *         filler class
    */
  private def existBNodeWithPropertyAndFiller(triples: RDD[Triple]): RDD[(Node, (Node, Node))] = {
      val existBNodesWithProperty = triples
        .filter(_.getPredicate == OWL2.onProperty.asNode())
        .map(trpl => (trpl.getSubject, trpl.getObject))

      val existBNodesWithFillerClass = triples
        .filter(_.getPredicate == OWL2.someValuesFrom.asNode())
        .map(trpl => (trpl.getSubject, trpl.getObject))

      existBNodesWithProperty.join(existBNodesWithFillerClass)
  }

  /**
    * Extracts subclass-or relations with a class intersection (of two atomic
    * classes) as subclass, i.e. Class1 \sqcap Class2 \sqsubseteq SuperClass .
    * These relations are returned as triplets (Class1, Class2, SuperClass).
    *
    * This method is not part of the ForwardRuleReasoner interface and should
    * only be called from within ForwardRuleReasonerEL. Otherwise the caching
    * (as it is implemented right) now won't work.
    *
    * @param triples Input Triple RDD
    * @return An RDD of triplets of the shape (Class1, Class2, SuperClass)
    */
  private[triples] def extractIntersectionSCORelations(triples: RDD[Triple]): RDD[(Node, Node, Node)] = {
    val intersectionClassesWithListBNodes =
      triples.filter(_.getPredicate == OWL2.intersectionOf.asNode())
        .map(trpl => (trpl.getSubject, trpl.getObject))

    val listBNodesWithFirstElems = triples.filter(_.getPredicate == RDF.first.asNode())
      .map(tripl => (tripl.getSubject, tripl.getObject))

    val listBNodesWithRestBNodes = triples.filter(_.getPredicate == RDF.rest.asNode())
      .map(trpl => (trpl.getSubject, trpl.getObject))

    val listBNodesWithFirstElemsAndRestBNodes =
      listBNodesWithFirstElems.join(listBNodesWithRestBNodes)
        .map[(Node, Node, Node)](e => (e._1, e._2._1, e._2._2))

    val listBNodesWithClasses: RDD[(Node, (Node, Node))] =
      listBNodesWithFirstElemsAndRestBNodes
        .map[(Node, (Node, Node))](e => (e._3, (e._1, e._2)))
        .join(listBNodesWithFirstElems)
        .map(e => (e._2._1._1, (e._2._1._2, e._2._2)))

    val intersections = intersectionClassesWithListBNodes.map(e => (e._2, e._1))
      .join(listBNodesWithClasses).map(e => (e._2._1, e._2._2))

    val subClassOfPairs = extractAtomicSubClassOf(triples)

    intersections.join(subClassOfPairs).map(e => (e._2._1._1, e._2._1._2, e._2._2))
  }

  /**
    * Extracts subclass-of relations with an existential restrictions as
    * subclass, i.e. \exists property.FillerClass \sqsubseteq SuperClass .
    * These relations are returned as triplets
    * (property, FillerClass, SuperClass).
    *
    * @param triples Input Triple RDD
    * @return An RDD of triplets of the shape (property, FillerClass, SuperClass)
    */
  private[triples] def extractExistentialSCORelations(triples: RDD[Triple]): RDD[(Node, Node, Node)] = {
    val existBNodesWithPropertyAndFiller = existBNodeWithPropertyAndFiller(triples)
    val subClassOfPairs = extractAtomicSubClassOf(triples)

    existBNodesWithPropertyAndFiller.join(subClassOfPairs)
      .map(e => (e._2._1._1, e._2._1._2, e._2._2))
  }

  /**
    * Extracts subclass-of relations with an existential restrictions as
    * superclass, i.e. SubClass \sqsubseteq \exists property.FillerClass .
    * These relations are returned as triplets
    * (SubClass, property, FillerClass).
    *
    * @param triples Input Triple RDD
    * @return An RDD of triplets of the shape (SubClass, property, FillerClass)
    */
  private[triples] def extractSCOExistentialRelations(triples: RDD[Triple]): RDD[(Node, Node, Node)] = {
    val existBNodesWithPropertyAndFiller = existBNodeWithPropertyAndFiller(triples)
    val subClassOfPairs = extractAtomicSubClassOf(triples)

    subClassOfPairs.map(e => (e._2, e._1)).join(existBNodesWithPropertyAndFiller)
      .map(e => (e._2._1, e._2._2._1, e._2._2._2))
  }

  /**
    * Extracts subproperty-of relations, i.e. subProp \sqsubseteq superProp .
    * The relations are returned as pairs (subProp, superProp).
    *
    * @param triples Input Triple RDD
    * @return An RDD of pairs of the shape (subProp, superProp)
    */
  private[triples] def extractSubPropertyOfRelations(triples: RDD[Triple]): RDD[(Node, Node)] = {
    triples.filter(_.getPredicate == RDFS.subPropertyOf.asNode())
      .map(trpl => (trpl.getSubject, trpl.getObject))
  }

  /**
    * Extracts property chain relations, i.e. axioms of the form
    * r1 o r1 \sqsubseteq s . The corresponding triple representations are a bit
    * more complicated:
    *
    *   :s owl:propertyChainAxiom _:1 .
    *   _:1 rdf:first :r1 .
    *   _:1 rdf:rest _:2 .
    *   _:2 rdf:first :r2 .
    *   _:2 rdf:rest rdf:nil .
    *
    * The relations are
    *
    * This method is not part of the ForwardRuleReasoner interface and should
    * only be called from within ForwardRuleReasonerEL.
    *
    * @param triples Input Triple RDD
    * @return An RDD of triplets of
    */
  private[triples] def extractPropertyChainRelations(triples: RDD[Triple]): RDD[(Node, Node, Node)] = {
    val superPropertiesWithListBNode =
      triples.filter(_.getPredicate == OWL2.propertyChainAxiom.asNode())
        .map(trpl => (trpl.getSubject, trpl.getObject))

    val listBNodesWithFirstElem =
      triples.filter(_.getPredicate == RDF.first.asNode())
        .map(trpl => (trpl.getSubject, trpl.getObject))

    // 'rest' can be the bNode of the rdf:rest part of the list or rdf:nil
    val listBNodesWithRest =
      triples.filter(_.getPredicate == RDF.rest.asNode())
        .map(trpl => (trpl.getSubject, trpl.getObject))

    val listBNodesWithSuperPropertyAndFirstElem =
      superPropertiesWithListBNode.map(pair => (pair._2, pair._1))
        .join(listBNodesWithFirstElem)

    // overall list bnode                         inner list/'rest' bnode
    //         v                                                  v
    // e.g. (_:23,((http://ex.com/superProp3,http://ex.com/cp5),_:42))
    val listBNodesWithSuperPropertyAndFirstElemAndRestBNode =
      listBNodesWithSuperPropertyAndFirstElem.join(listBNodesWithRest)

    listBNodesWithSuperPropertyAndFirstElemAndRestBNode
      // (listBnode, ((superProp, firstElem), restBNode))
      //      => (restBNode, (listBNode, superProp, firstElem))
      .map(e => (e._2._2, (e._1, e._2._1._1, e._2._1._2)))
      .join(listBNodesWithFirstElem)
      // (restBNode, ((listBNode, superProp, firstElem), secondElem))
      //      => (firstElem, secondElem, superProp)
      .map(e => (e._2._1._3, e._2._2, e._2._1._2))
  }
}
