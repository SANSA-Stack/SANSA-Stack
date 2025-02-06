package net.sansa_stack.inference.spark.forwardchaining.axioms

import net.sansa_stack.inference.utils.CollectionUtils
import net.sansa_stack.owl.spark.owlAxioms
import net.sansa_stack.owl.spark.rdd.{FunctionalSyntaxOWLAxiomsRDDBuilder, OWLAxiomsRDD}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.AxiomType
import org.semanticweb.owlapi.model._

import scala.jdk.CollectionConverters._

/**
  * A forward chaining implementation for the OWL Horst entailment regime that works
  * on OWL axioms.
  *
  * Rule names refer to the name scheme used in
  * 'RORS: Enhanced Rule-based OWL Reasoning on Spark' by Liu, Feng, Zhang,
  * Wang, Rao
  *
  * @param sc The Apache Spark context
  * @param parallelism The degree of parallelism
  * @author Heba Mohamed
  */
class ForwardRuleReasonerOWLHorst (sc: SparkContext, parallelism: Int = 30) extends TransitiveReasoner{

  def this(sc: SparkContext) = this(sc, sc.defaultParallelism)

  def apply(axioms: RDD[OWLAxiom]): RDD[OWLAxiom] = {

    val axiomsRDD = axioms.cache() // cache this RDD because it will be used quite often

    val manager = OWLManager.createOWLOntologyManager()
    val dataFactory = manager.getOWLDataFactory

    val startTime = System.currentTimeMillis()

    // ------------ extract the schema elements -------------------
//    val classes: RDD[OWLClass] = axiomsRDD.flatMap {
//      case axiom: HasClassesInSignature => axiom.classesInSignature().iterator().asScala
//      case _ => null
//    }.filter(_ != null).distinct()

    var subClassof = owlAxioms.extractAxioms(axiomsRDD, AxiomType.SUBCLASS_OF)
    var subDataProperty = owlAxioms.extractAxioms(axiomsRDD, AxiomType.SUB_DATA_PROPERTY)
    var subObjProperty = owlAxioms.extractAxioms(axiomsRDD, AxiomType.SUB_OBJECT_PROPERTY)
    val subAnnProp = owlAxioms.extractAxioms(axiomsRDD, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
    val objectProDomain = owlAxioms.extractAxioms(axiomsRDD, AxiomType.OBJECT_PROPERTY_DOMAIN)
    val objectProRange = owlAxioms.extractAxioms(axiomsRDD, AxiomType.OBJECT_PROPERTY_RANGE)
    val dataProDomain = owlAxioms.extractAxioms(axiomsRDD, AxiomType.DATA_PROPERTY_DOMAIN)
    val dataProRange = owlAxioms.extractAxioms(axiomsRDD, AxiomType.DATA_PROPERTY_RANGE)
    val annProDomain = owlAxioms.extractAxioms(axiomsRDD, AxiomType.ANNOTATION_PROPERTY_DOMAIN)
    val equClass = owlAxioms.extractAxioms(axiomsRDD, AxiomType.EQUIVALENT_CLASSES)
    val equDataProp = owlAxioms.extractAxioms(axiomsRDD, AxiomType.EQUIVALENT_DATA_PROPERTIES)
    val equObjProp = owlAxioms.extractAxioms(axiomsRDD, AxiomType.EQUIVALENT_OBJECT_PROPERTIES)

    // --------- extract instance elements
    var classAssertion = owlAxioms.extractAxioms(axiomsRDD, AxiomType.CLASS_ASSERTION)

    var dataPropAssertion = owlAxioms.extractAxioms(axiomsRDD, AxiomType.DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    var objPropAssertion = owlAxioms.extractAxioms(axiomsRDD, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]

    var annAssertion = owlAxioms.extractAxioms(axiomsRDD, AxiomType.ANNOTATION_ASSERTION)
      .asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]

    // ---------------- Schema Rules --------------------
    // 1. we have to process owl:equivalentClass (resp. owl:equivalentProperty) before computing the transitive closure
    // of rdfs:subClassOf (resp. rdfs:sobPropertyOf)
    // O11a: (C owl:equivalentClass D) -> (C rdfs:subClassOf D )
    // O11b: (C owl:equivalentClass D) -> (D rdfs:subClassOf C )

    val equClass_Pairs = equClass.asInstanceOf[RDD[OWLEquivalentClassesAxiom]]
      .flatMap(equivClassesAxiom => equivClassesAxiom.asPairwiseAxioms().asScala)

    val subC1 = equClass_Pairs
      .map(a => dataFactory.getOWLSubClassOfAxiom(a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))

    val subC2 = equClass_Pairs
      .map(a => dataFactory.getOWLSubClassOfAxiom(a.getOperandsAsList.get(1), a.getOperandsAsList.get(0)))

    subClassof = sc.union(subClassof,
      subC1.asInstanceOf[RDD[OWLAxiom]],
      subC2.asInstanceOf[RDD[OWLAxiom]])
        .distinct(parallelism)

    // 2. we compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf
    // R1: (x rdfs:subClassOf y), (y rdfs:subClassOf z) -> (x rdfs:subClassOf z)

    val tr = new TransitiveReasoner()

    val subClassOfAxiomsTrans = tr.computeTransitiveClosure(subClassof, AxiomType.SUBCLASS_OF)
        .asInstanceOf[RDD[OWLSubClassOfAxiom]]
        .filter(a => a.getSubClass != a.getSuperClass) // to exclude axioms with (C owl:subClassOf C)

    // Convert all RDDs into maps which should be more efficient later on
    val subClassMap = CollectionUtils
      .toMultiMap(subClassOfAxiomsTrans.asInstanceOf[RDD[OWLSubClassOfAxiom]]
        .map(a => (a.getSubClass, a.getSuperClass)).collect())

    val dataDomainMap = dataProDomain.asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
      .map(a => (a.getProperty, a.getDomain)).collect().toMap

    val objDomainMap = objectProDomain.asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .map(a => (a.getProperty, a.getDomain)).collect().toMap

    val dataRangeMap = dataProRange.asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .map(a => (a.getProperty, a.getRange)).collect().toMap

    val objRangeMap = objectProRange.asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .map(a => (a.getProperty, a.getRange)).collect().toMap

    val annDomainMap = annProDomain.asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .map(a => (a.getProperty, a.getDomain)).collect().toMap

    val equClassMap = equClass_Pairs
      .map(a => (a.getOperandsAsList.get(0), a.getOperandsAsList.get(1))).collect.toMap

    // distribute the schema data structures by means of shared variables
    // Assume that the schema data is less than the instance data

    val subClassOfBC = sc.broadcast(subClassMap)
    val dataDomainMapBC = sc.broadcast(dataDomainMap)
    val objDomainMapBC = sc.broadcast(objDomainMap)
    val annDomainMapBC = sc.broadcast(annDomainMap)
    val dataRangeMapBC = sc.broadcast(dataRangeMap)
    val objRangeMapBC = sc.broadcast(objRangeMap)
    val equClassMapBC = sc.broadcast(equClassMap)
   // val equClassSwapMapBC = sc.broadcast(equClassSwapMap)

    // Compute the equivalence of classes and properties
    // O11c: (C rdfs:subClassOf D ), (D rdfs:subClassOf C ) -> (C owl:equivalentClass D)
    val eqClass = subClassOfAxiomsTrans
      .filter(a => subClassOfBC.value.getOrElse(a.getSuperClass, Set.empty).contains(a.getSubClass))
      .map(a => dataFactory.getOWLEquivalentClassesAxiom(a.getSubClass, a.getSuperClass))

    // for equivelantDataProperty and equivelantObjectProperty
    // O12a: (C owl:equivalentProperty D) -> (C rdfs:subPropertyOf D )
    // O12b: (C owl:equivalentProperty D) -> (D rdfs:subPropertyOf C )

    val subDProp_Pairs = equDataProp.asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .flatMap(eq => eq.asPairwiseAxioms().asScala)

    val subDProp1 = subDProp_Pairs
      .map(a => dataFactory.getOWLSubDataPropertyOfAxiom(a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))

    val subDProp2 = subDProp_Pairs
      .map(a => dataFactory.getOWLSubDataPropertyOfAxiom(a.getOperandsAsList.get(1), a.getOperandsAsList.get(0)))

    subDataProperty = sc.union(subDataProperty,
      subDProp1.asInstanceOf[RDD[OWLAxiom]],
      subDProp2.asInstanceOf[RDD[OWLAxiom]])
      .distinct(parallelism)

    val subOProp_Pairs = equObjProp.asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .flatMap(eq => eq.asPairwiseAxioms().asScala)

    val subOProp1 = subOProp_Pairs
      .map(a => dataFactory.getOWLSubObjectPropertyOfAxiom(a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))

    val subOProp2 = subOProp_Pairs
      .map(a => dataFactory.getOWLSubObjectPropertyOfAxiom(a.getOperandsAsList.get(1), a.getOperandsAsList.get(0)))

    subObjProperty = sc.union(subObjProperty,
      subOProp1.asInstanceOf[RDD[OWLAxiom]],
      subOProp2.asInstanceOf[RDD[OWLAxiom]])
      .distinct(parallelism)

    // R2: (x rdfs:subPropertyOf y), (y rdfs:subPropertyOf z) -> (x rdfs:subPropertyOf z)
    // Apply R2 for OWLSubDataProperty and OWLSubObjectProperty
    val subDataPropOfAxiomsTrans = tr.computeTransitiveClosure(subDataProperty, AxiomType.SUB_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .filter(a => a.getSubProperty != a.getSuperProperty) // to exclude axioms with (C owl:subDataPropertyOf C)

    val subObjPropOfAxiomsTrans = tr.computeTransitiveClosure(subObjProperty, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .filter(a => a.getSubProperty != a.getSuperProperty) // to exclude axioms with (C owl:subObjectPropertyOf C)

    val subAnnPropOfAxiomsTrans = tr.computeTransitiveClosure(subAnnProp, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
      .asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .filter(a => a.getSubProperty != a.getSuperProperty) // to exclude axioms with (C owl:subAnnotationPropertyOf C)

    val subDataPropMap = CollectionUtils
      .toMultiMap(subDataPropOfAxiomsTrans.asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    val subObjectPropMap = CollectionUtils
      .toMultiMap(subObjPropOfAxiomsTrans.asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    val subAnnPropMap = CollectionUtils
      .toMultiMap(subAnnPropOfAxiomsTrans.asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    val subDataPropertyBC = sc.broadcast(subDataPropMap)
    val subObjectPropertyBC = sc.broadcast(subObjectPropMap)
    val subAnnPropertyBC = sc.broadcast(subAnnPropMap)

    // O12c: (C rdfs:subPropertyOf D), (D rdfs:subPropertyOf C) -> (C owl:equivalentProperty D)
    // Apply O12c for OWLSubDataProperty and OWLSubObjectProperty
    val eqDP = subDataPropOfAxiomsTrans
      .filter(a => subDataPropertyBC.value.getOrElse(a.getSuperProperty, Set.empty).contains(a.getSubProperty))
      .map(a => dataFactory.getOWLEquivalentDataPropertiesAxiom(a.getSubProperty, a.getSuperProperty))

    val eqOP = subObjPropOfAxiomsTrans
      .filter(a => subObjectPropertyBC.value.getOrElse(a.getSuperProperty, Set.empty).contains(a.getSubProperty))
      .map(a => dataFactory.getOWLEquivalentObjectPropertiesAxiom(a.getSubProperty, a.getSuperProperty))

    // Extract properties with certain OWL characteristic and broadcast
    val functionalObjPropBC = sc.broadcast(
      owlAxioms.extractAxioms(axiomsRDD, AxiomType.FUNCTIONAL_OBJECT_PROPERTY)
        .asInstanceOf[RDD[OWLFunctionalObjectPropertyAxiom]]
        .map(a => a.getProperty)
        .collect())

    val inversefunctionalObjPropBC = sc.broadcast(
      owlAxioms.extractAxioms(axiomsRDD, AxiomType.INVERSE_FUNCTIONAL_OBJECT_PROPERTY)
        .asInstanceOf[RDD[OWLInverseFunctionalObjectPropertyAxiom]]
        .map(a => a.getProperty)
        .collect())

    val symmetricObjPropBC = sc.broadcast(
      owlAxioms.extractAxioms(axiomsRDD, AxiomType.SYMMETRIC_OBJECT_PROPERTY)
        .asInstanceOf[RDD[OWLSymmetricObjectPropertyAxiom]]
        .map(a => a.getProperty)
        .collect())

    val transtiveObjPropBC = sc.broadcast(
      owlAxioms.extractAxioms(axiomsRDD, AxiomType.TRANSITIVE_OBJECT_PROPERTY)
        .asInstanceOf[RDD[OWLTransitiveObjectPropertyAxiom]]
        .map(a => a.getProperty)
        .collect())

    val inverseObjPropBC = sc.broadcast(
      owlAxioms.extractAxioms(axiomsRDD, AxiomType.INVERSE_OBJECT_PROPERTIES)
        .asInstanceOf[RDD[OWLInverseObjectPropertiesAxiom]]
        .map(a => (a.getFirstProperty, a.getSecondProperty))
        .collect().toMap)

    val swapInverseObjPropBC = sc.broadcast(inverseObjPropBC.value.map(_.swap))

    val allAxioms = sc.union(axioms, subClassof, subDataProperty, subObjProperty, subAnnProp,
      subClassOfAxiomsTrans.asInstanceOf[RDD[OWLAxiom]],
      subObjPropOfAxiomsTrans.asInstanceOf[RDD[OWLAxiom]],
      subDataPropOfAxiomsTrans.asInstanceOf[RDD[OWLAxiom]],
      subAnnPropOfAxiomsTrans.asInstanceOf[RDD[OWLAxiom]],
      eqClass.asInstanceOf[RDD[OWLAxiom]],
      eqDP.asInstanceOf[RDD[OWLAxiom]],
      eqOP.asInstanceOf[RDD[OWLAxiom]])
      .distinct(parallelism)

    // split ontology Axioms based on type, sameAs, and the rest of axioms
    var typeAxioms = allAxioms.filter{axiom => axiom.getAxiomType.equals(AxiomType.CLASS_ASSERTION)}
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]

    var sameAsAxioms = allAxioms.filter(axiom => axiom.getAxiomType.equals(AxiomType.SAME_INDIVIDUAL))
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]

    var SPOAxioms = allAxioms.subtract(typeAxioms.asInstanceOf[RDD[OWLAxiom]])
      .subtract(sameAsAxioms.asInstanceOf[RDD[OWLAxiom]])

    val eq: RDD[OWLClassExpression] = eqClass.map(a => a.getOperandsAsList.get(1))

    // More OWL vocabulary used in property restrictions
    // onProperty, hasValue, allValuesFrom, someValuesFrom

    val dataHasVal = eq.filter(a => a.getClassExpressionType.equals(ClassExpressionType.DATA_HAS_VALUE))
    val objHasVal = eq.filter(a => a.getClassExpressionType.equals(ClassExpressionType.OBJECT_HAS_VALUE))
    val objAllValues = eq.filter(a => a.getClassExpressionType.equals(ClassExpressionType.OBJECT_ALL_VALUES_FROM))
    val objSomeValues = eq.filter(a => a.getClassExpressionType.equals(ClassExpressionType.OBJECT_SOME_VALUES_FROM))

    val dataHasValBC = sc.broadcast(dataHasVal.asInstanceOf[RDD[OWLDataHasValue]]
      .map(a => (a.getProperty, a.getFiller)).collect().toMap)

    val objHasValBC = sc.broadcast(objHasVal.asInstanceOf[RDD[OWLObjectHasValue]]
      .map(a => (a.getProperty, a.getFiller)).collect().toMap)

    val objAllValuesBC = sc.broadcast(objAllValues.asInstanceOf[RDD[OWLObjectAllValuesFrom]]
      .map(a => (a.getProperty, a.getFiller)).collect().toMap)

    val objSomeValuesBC = sc.broadcast(objSomeValues.asInstanceOf[RDD[OWLObjectSomeValuesFrom]]
      .map(a => (a.getProperty, a.getFiller)).collect().toMap)

    val objSomeValuesSwapBC = sc.broadcast(objSomeValues.asInstanceOf[RDD[OWLObjectSomeValuesFrom]]
      .map(a => (a.getFiller, a.getProperty)).collect().toMap)

    // Perform fix-point iteration i.e. we process a set of rules until no new data has been generated

    var newData = true
    var newTypeCount = 0L
    var newAxiomsCount = 0L
    var i = 0

    while (newData) {
      i += 1

      // -------------------- SPO Rules -----------------------------
      // O3: (?P rdf:type owl:SymmetricProperty), (?X ?P ?Y) -> (?Y ?P ?X)

      val O3 = objPropAssertion
        .filter(a => symmetricObjPropBC.value.contains(a.getProperty))
        .map(a => dataFactory.getOWLObjectPropertyAssertionAxiom(a.getProperty, a.getObject, a.getSubject))

      // 2. SubPropertyOf inheritance according to R3 is computed
      // R3: a rdfs:subPropertyOf b . x a y  -> x b y .

      val R3a = dataPropAssertion
        .filter(a => subDataPropertyBC.value.contains(a.getProperty))
        .flatMap(a => subDataPropertyBC.value(a.getProperty)
          .map(s => dataFactory.getOWLDataPropertyAssertionAxiom(s, a.getSubject, a.getObject)))
        .setName("R3a")

      val R3b = objPropAssertion
        .filter(a => subObjectPropertyBC.value.contains(a.getProperty))
        .flatMap(a => subObjectPropertyBC.value(a.getProperty)
          .map(s => dataFactory.getOWLObjectPropertyAssertionAxiom(s, a.getSubject, a.getObject)))
        .setName("R3b")

      val R3c = annAssertion
        .filter(a => subAnnPropertyBC.value.contains(a.getProperty))
        .flatMap(a => subAnnPropertyBC.value(a.getProperty)
          .map(s => dataFactory.getOWLAnnotationAssertionAxiom(s, a.getSubject, a.getValue)))
        .setName("R3c")

      // O7a: (P owl:inverseOf Q), (X P Y) -> (Y Q X)
      val O7a = objPropAssertion
        .filter(a => inverseObjPropBC.value.contains(a.getProperty))
        .map(a => dataFactory
          .getOWLObjectPropertyAssertionAxiom(inverseObjPropBC.value(a.getProperty), a.getObject, a.getSubject))

      // O7b: (P owl:inverseOf Q), (X Q Y) -> (Y P X)
      val O7b = objPropAssertion
        .filter(a => swapInverseObjPropBC.value.contains(a.getProperty))
        .map(a => dataFactory
          .getOWLObjectPropertyAssertionAxiom(swapInverseObjPropBC.value(a.getProperty), a.getObject, a.getSubject))

      dataPropAssertion = dataPropAssertion.union(R3a)
      objPropAssertion = objPropAssertion.union(O3).union(R3b).union(O7a).union(O7b)
      annAssertion = annAssertion.union(R3c)

      var newAxioms = new UnionRDD(sc, Seq(R3a.asInstanceOf[RDD[OWLAxiom]],
        objPropAssertion.asInstanceOf[RDD[OWLAxiom]],
        annAssertion.asInstanceOf[RDD[OWLAxiom]]))
        .distinct(parallelism)
        .subtract(SPOAxioms, parallelism)

      newAxiomsCount = newAxioms.count()

      if (i == 1 || newAxiomsCount > 0) {

        SPOAxioms = SPOAxioms.union(newAxioms).distinct(parallelism)

        // O4: (P rdf:type owl:TransitiveProperty), (X P Y), (Y P Z) -> (X P Z)
        val O4_a = objPropAssertion.filter(a => transtiveObjPropBC.value.contains(a.getProperty))

        val O4 = tr.computeTransitiveClosure(O4_a)

         objPropAssertion = objPropAssertion.union(O4)
         SPOAxioms = SPOAxioms.union(O4.asInstanceOf[RDD[OWLAxiom]])
      }

      // SPOAxioms = SPOAxioms.union(newAxioms).distinct(parallelism)

      // ---------------------- Type Rules -------------------------------
      // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed
      // R4:  a rdfs:domain b . x a y  -->  x rdf:type b

      val R4a = dataPropAssertion // .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
        .filter(a => dataDomainMapBC.value.contains(a.getProperty))
        .map(a => dataFactory.getOWLClassAssertionAxiom(dataDomainMapBC.value(a.getProperty), a.getSubject))
        .setName("R4a")

      val R4b = objPropAssertion
        .filter(a => objDomainMapBC.value.contains(a.getProperty))
        .map(a => dataFactory.getOWLClassAssertionAxiom(objDomainMapBC.value(a.getProperty), a.getSubject))
        .setName("R4b")

      val R4c = annAssertion
        .filter(a => annDomainMapBC.value.contains(a.getProperty))
        .map(a => dataFactory
          .getOWLClassAssertionAxiom(dataFactory.getOWLClass(annDomainMapBC.value(a.getProperty)),
            dataFactory.getOWLNamedIndividual(a.getSubject.toString)))
        .setName("R4c")

      // R5: a rdfs:range x . y a z  -->  z rdf:type x .

      val R5a = dataPropAssertion.filter(a => dataRangeMapBC.value.contains(a.getProperty) && !a.getObject.isLiteral) // Add checking for non-literals
        .map(a => dataFactory.getOWLClassAssertionAxiom
      (dataRangeMapBC.value(a.getProperty).asInstanceOf[OWLClassExpression], a.getObject.asInstanceOf[OWLIndividual]))
        .setName("R5a")

      val R5b = objPropAssertion
        .filter(a => objRangeMapBC.value.contains(a.getProperty))
        .map(a => dataFactory.getOWLClassAssertionAxiom(objRangeMapBC.value(a.getProperty), a.getObject))
        .setName("R5b")

      // 4. SubClass inheritance according to rdfs9
      // R6: x rdfs:subClassOf y . z rdf:type x -->  z rdf:type y .
      val R6 = typeAxioms
        .filter(a => subClassOfBC.value.contains(a.getClassExpression))
        .flatMap(a => subClassOfBC.value(a.getClassExpression)
          .map(s => dataFactory.getOWLClassAssertionAxiom(s, a.getIndividual)))
        .setName("R6")

      // O14: (R owl:hasValue V),(R owl:onProperty P),(X rdf:type R) -> (X P V)
      // in OWLAxioms we have 3 cases where we can find OWLDataHasValue

      // case a: OWLClassAssertion(OWLDataHasValue(P, w), i)
      val O14_data_a = typeAxioms.map(a => (a.getClassExpression, a.getIndividual))
        .filter(a => a._1.isInstanceOf[OWLDataHasValue])
        .map(a => (a._1.asInstanceOf[OWLDataHasValue], a._2))
        .map(a => dataFactory.getOWLDataPropertyAssertionAxiom(a._1.getProperty, a._2, a._1.getFiller))

      // case b: OWLSubClassOf(S, OWLDataHasValue(P, w)) , OWLClassAssertion(S, i)
      val subOperands = subClassof.asInstanceOf[RDD[OWLSubClassOfAxiom]]
        .map(a => (a.getSubClass, a.getSuperClass))

      val sd: RDD[(OWLClassExpression, OWLDataHasValue)] = subOperands.filter(s => s._2.isInstanceOf[OWLDataHasValue])
        .map(s => (s._1, s._2.asInstanceOf[OWLDataHasValue]))   // (S, OWLDataHasValue(P, w))

      val O14_data_b = typeAxioms.filter(a => subClassOfBC.value.contains(a.getClassExpression))
        .map(a => (a.getClassExpression, a.getIndividual))    // (S, i)
        .join(sd)   // (S, (i, (OWLDataHasValue(P, w))))
        .filter(x => x._2._2.isInstanceOf[OWLDataHasValue])
        .map(a => dataFactory.getOWLDataPropertyAssertionAxiom(a._2._2.getProperty, a._2._1, a._2._2.getFiller))

      // case c: OWLEquivelantClass(E, OWLDataHasValue(P, w)), OWLClassAssertion(E, i)
      val eqOperands = equClass.asInstanceOf[RDD[OWLEquivalentClassesAxiom]]
        .map(a => (a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))

      val e = eqOperands.filter(eq => eq._2.isInstanceOf[OWLDataHasValue])
        .map(eq => (eq._1, eq._2.asInstanceOf[OWLDataHasValue]))

      val O14_data_c = typeAxioms.filter(a => equClassMapBC.value.contains(a.getClassExpression))
        .map(a => (a.getClassExpression, a.getIndividual))
        .join(e).filter(x => x._2._2.isInstanceOf[OWLDataHasValue])
        .map(a => dataFactory.getOWLDataPropertyAssertionAxiom(a._2._2.getProperty, a._2._1, a._2._2.getFiller))

      val O14_data = O14_data_a.union(O14_data_b).union(O14_data_c).distinct(parallelism)

      dataPropAssertion = dataPropAssertion.union(O14_data)

      // O14: (R owl:hasValue V),(R owl:onProperty P),(X rdf:type R) -> (X P V)
      // we have the same 3 cases where we can find OWLObjectHasValue

      // case a: OWLClassAssertion(OWLObjectHasValue(P, w), i)
      val O14_obj_a = typeAxioms.map(a => (a.getClassExpression, a.getIndividual))
        .filter(a => a._1.isInstanceOf[OWLObjectHasValue])
        .map(a => (a._1.asInstanceOf[OWLObjectHasValue], a._2))
        .map(a => dataFactory.getOWLObjectPropertyAssertionAxiom(a._1.getProperty, a._2, a._1.getFiller))

      // case b: OWLSubClassOf(S, OWLObjectHasValue(P, w)) , OWLClassAssertion(S, i)
      val so = subOperands.filter(s => s._2.isInstanceOf[OWLObjectHasValue])
        .map(s => (s._1, s._2.asInstanceOf[OWLObjectHasValue]))

      val O14_obj_b = typeAxioms.filter(a => subClassOfBC.value.contains(a.getClassExpression))
        .map(a => (a.getClassExpression, a.getIndividual))
        .join(so).filter(x => x._2._2.isInstanceOf[OWLObjectHasValue])
        .map(a => dataFactory.getOWLObjectPropertyAssertionAxiom(a._2._2.getProperty, a._2._1, a._2._2.getFiller))

      // case c: OWLEquivelantClass(E, OWLObjectHasValue(P, w)), OWLClassAssertion(E, i)
      val eo = eqOperands.filter(eq => eq._2.isInstanceOf[OWLObjectHasValue])
        .map(eq => (eq._1, eq._2.asInstanceOf[OWLObjectHasValue]))

      val O14_obj_c = typeAxioms.filter(a => equClassMapBC.value.contains(a.getClassExpression))
        .map(a => (a.getClassExpression, a.getIndividual))
        .join(eo).filter(x => x._2._2.isInstanceOf[OWLObjectHasValue])
        .map(a => dataFactory.getOWLObjectPropertyAssertionAxiom(a._2._2.getProperty, a._2._1, a._2._2.getFiller))

      val O14_obj = O14_obj_a.union(O14_obj_b).union(O14_obj_c)

      objPropAssertion = objPropAssertion.union(O14_obj).distinct(parallelism)

      // O13: (R owl:hasValue V), (R owl:onProperty P), (U P V) -> (U rdf:type R)
      // case a: OWLEquivalentClasses(E, OWLDataHasValue(P, w)), OWLDataPropertyAssertion(P, i, w) --> OWLClassAssertion(E, i)

      val e_swap = e.map(a => (a._2.getProperty, a._1))   // (P, E)
      val O13_data_a = dataPropAssertion.filter(a => dataHasValBC.value.contains(a.getProperty))
        .map(a => (a.getProperty, a.getSubject))    // (P, i)
        .join(e_swap)   //  (P, (E, i))
        .map(a => dataFactory.getOWLClassAssertionAxiom(a._2._2, a._2._1))

      // case b: OWLSubClassOf(S, OWLDataHasValue(P, w)) , OWLClassAssertion(S, i)
      val s_swap = sd.map(a => (a._2.getProperty, a._1))    // (P, S)
      val O13_data_b = dataPropAssertion.map(a => (a.getProperty, a.getSubject))    // (P, U)
        .join(s_swap) // (P, (S, U))
        .map(a => dataFactory.getOWLClassAssertionAxiom(a._2._2, a._2._1))

      // case a: OWLEquivalentClasses(E, OWLObjectHasValue(P, w)), OWLObjectPropertyAssertion(P, i, w) --> OWLClassAssertion(E, i)
      val eo_swap = eo.map(a => (a._2.getProperty, a._1))
      val O13_obj_a = objPropAssertion.filter(a => objHasValBC.value.contains(a.getProperty))
        .map(a => (a.getProperty, a.getSubject))
        .join(eo_swap)
        .map(a => dataFactory.getOWLClassAssertionAxiom(a._2._2, a._2._1))

      // case b: OWLSubClassOf(S, OWLObjectHasValue(P, w)) , OWLClassAssertion(S, i)
      val so_swap = so.map(a => (a._2.getProperty, a._1))
      val O13_obj_b = objPropAssertion.map(a => (a.getProperty, a.getSubject))
        .join(so_swap)
        .map(a => dataFactory.getOWLClassAssertionAxiom(a._2._2, a._2._1))

 //     typeAxioms = new UnionRDD(sc, Seq(typeAxioms, O13_data_a, O13_data_b, O13_obj_a, O13_obj_b))
 //       .distinct(parallelism)

      // Apply O15, O16 only on ObjectAssertions because we compare individuals

      // O15: (c owl:someValuesFrom w), (c owl:onProperty P), (u P x), (x rdf:type w) -> (u rdf:type c)
      val e1 = eqOperands.filter(eq => eq._2.isInstanceOf[OWLObjectSomeValuesFrom])
             .map(eq => (eq._2.asInstanceOf[OWLObjectSomeValuesFrom], eq._1))
             .map(eq => (eq._1.getFiller, (eq._1.getProperty, eq._2))) // (w, (p, c))

      val O15_obj_1 = objPropAssertion.filter(a => objSomeValuesBC.value.contains(a.getProperty))
             .map(a => (a.getObject, a.getSubject)) // (x, u)

      val O15_obj_2 = typeAxioms.filter(a => objSomeValuesSwapBC.value.contains(a.getClassExpression))
             .map(a => (a.getIndividual, a.getClassExpression)) // (x, w)
             .join(O15_obj_1) // (x, (w, u))

      val O15 = O15_obj_2.map(a => (a._2._1, (a._1, a._2._2)))   // (w, (x, u))
               .join(e1) // join to get the equivalent class (w, ((x, u), (p, c)))
               .map(a => dataFactory.getOWLClassAssertionAxiom(a._2._2._2, a._2._1._2))
               .distinct(parallelism)

      // O16: (v owl:allValuesFrom w), (v owl:onProperty P), (u P x), (u rdf:type v) -> (x rdf:type w)
      val e2 = eqOperands.filter(eq => eq._2.isInstanceOf[OWLObjectAllValuesFrom])
        .map(eq => (eq._1, eq._2.asInstanceOf[OWLObjectAllValuesFrom]))
        .map(eq => (eq._1, eq._2.getFiller)) // (v, w)

      val O16_1 = objPropAssertion.filter(a => objAllValuesBC.value.contains(a.getProperty))
        .map(a => (a.getSubject, a.getObject)) // (u, x)

      val O16_2 = typeAxioms.filter(a => equClassMapBC.value.contains(a.getClassExpression))
        .map(a => (a.getIndividual, a.getClassExpression)) // (u, v)

      val O16 = O16_1.join(O16_2) // (u, (x, v))
        .map(a => (a._2._2, a._2._1))  // (v, x)
        .join(e2)    // (v, (x, w))
        .map(a => dataFactory.getOWLClassAssertionAxiom(a._2._2, a._2._1))
        .distinct(parallelism)

      val newTypeAxioms = new UnionRDD(sc, Seq(R4a, R4b, R4c, R5a, R5b, R6,
        O13_data_a, O13_data_b, O13_obj_a, O13_obj_b, O15, O16))
        .distinct(parallelism).subtract(typeAxioms, parallelism)

      newTypeCount = newTypeAxioms.count()

      if(newTypeCount > 0) {

        typeAxioms = typeAxioms.union(newTypeAxioms) // add type axioms
      }

      newData = newTypeCount > 0 || newAxiomsCount > 0

      newAxioms = sc.union(newAxioms, O14_data.asInstanceOf[RDD[OWLAxiom]], O14_obj.asInstanceOf[RDD[OWLAxiom]])
        .distinct(parallelism)
      newAxiomsCount = newAxioms.count()

      SPOAxioms = sc.union(SPOAxioms, newAxioms).distinct(parallelism)
      typeAxioms = sc.union(typeAxioms, newTypeAxioms).distinct(parallelism)
    }

//    val infered: Long = allAxioms.count - newTypeCount - newAxiomsCount
//    println("\n Finish with " + infered + " Inferred Axioms from Schema, SPO and Type rules")

    // --------------------- SameAs Rules --------------------------

    // O1: (P rdf:type owl:FunctionalProperty), (A P B), notLiteral(B), (A P C), notLiteral(C), notEqual(B C) -> (B owl:sameAs C)
    // this rule performed only on OWLObjectAssertions because we compare individuals not literals
    val O1_a = objPropAssertion.filter(a => functionalObjPropBC.value.contains(a.getProperty))
      .map(a => ((a.getProperty, a.getSubject), a.getObject))     // ((P, A), B)

    // perform self join
    val O1 = O1_a.join(O1_a)      // ((P, A), (B, C))
      .filter(a => a._2._1 != a._2._2)
      .map(a => dataFactory.getOWLSameIndividualAxiom(a._2._1, a._2._2))      // B owl:sameAs C
      .distinct(parallelism)

    // O2: (P rdf:type owl:InverseFunctionalProperty), (A P B), (C P B), notEqual(A C) -> (A owl:sameAs C)
    val O2_a = objPropAssertion.filter(a => inversefunctionalObjPropBC.value.contains(a.getProperty))
      .map(a => ((a.getProperty, a.getObject), a.getSubject))     // ((P, B), A)

    // perform self join
    val O2 = O2_a.join(O2_a)      // ((P, B), (A, C))
      .filter(a => a._2._1 != a._2._2)
      .map(a => dataFactory.getOWLSameIndividualAxiom(a._2._1, a._2._2))      // A owl:sameAs C
      .distinct(parallelism)

    val newSameAsAxioms = sc.union(O1, O2)
    sameAsAxioms = sc.union(sameAsAxioms, newSameAsAxioms).distinct(parallelism)

    val time = System.currentTimeMillis() - startTime

    val inferredAxioms = sc.union(typeAxioms.asInstanceOf[RDD[OWLAxiom]], sameAsAxioms.asInstanceOf[RDD[OWLAxiom]], SPOAxioms)
      .subtract(axioms)
      .distinct(parallelism)

    println("\n Finish with " + inferredAxioms.count + " Inferred Axioms after adding SameAs rules")
    println("\n...finished materialization in " + (time/1000) + " sec.")

    val inferredGraph = inferredAxioms.union(axioms).distinct(parallelism)

//    inferredAxioms
    inferredGraph
  }

}

//
 object ForwardRuleReasonerOWLHorst {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
     //  .master("spark://172.18.160.16:3090")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("OWLAxiom Forward Chaining Rule Reasoner")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    //    val sparkConf = new SparkConf().setMaster("spark://172.18.160.16:3077")

     val input = getClass.getResource("/ont_functional.owl").getPath
       // val input = args(0)

    println("=====================================")
    println("|  OWLAxioms Forward Rule Reasoner  |")
    println("=====================================")

    // Call the functional syntax OWLAxiom builder
    val owlAxiomsRDD: OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession, input).distinct()

    val ruleReasoner = new ForwardRuleReasonerOWLHorst(sc, 2)
    val res: RDD[OWLAxiom] = ruleReasoner(owlAxiomsRDD)

    sparkSession.stop
  }
 }
