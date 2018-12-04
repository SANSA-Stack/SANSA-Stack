package net.sansa_stack.inference.spark.forwardchaining.axioms

import net.sansa_stack.inference.utils.CollectionUtils
import net.sansa_stack.owl.spark.rdd.{FunctionalSyntaxOWLAxiomsRDDBuilder, OWLAxiomsRDD}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._

import scala.collection.JavaConverters._

/**
  * A forward chaining implementation for the OWL Horst entailment regime that works
  * on OWL axioms
  *
  * @param sc The Apache Spark context
  * @param parallelism The degree of parallelism
  * @author Heba Mohamed
  */


class ForwardRuleReasonerOWLHorst (sc: SparkContext, parallelism: Int = 2) extends TransitiveReasoner{

  def this(sc: SparkContext) = this(sc, sc.defaultParallelism)

  def apply(axioms: RDD[OWLAxiom], input: String): RDD[OWLAxiom] = {

    val startTime = System.currentTimeMillis()

    val manager = OWLManager.createOWLOntologyManager()
    val dataFactory = manager.getOWLDataFactory

    val axiomsRDD = axioms.cache() // cache this RDD because it will be used quite often

    // ------------ extract the schema elements -------------------
    val classes: RDD[OWLClass] = axiomsRDD.flatMap {
      case axiom: HasClassesInSignature => axiom.classesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    var subClassof = extractAxiom(axiomsRDD, AxiomType.SUBCLASS_OF)
    var subDataProperty = extractAxiom(axiomsRDD, AxiomType.SUB_DATA_PROPERTY)
    var subObjProperty = extractAxiom(axiomsRDD, AxiomType.SUB_OBJECT_PROPERTY)
    var subAnnProp = extractAxiom(axiomsRDD, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
    val objectProDomain = extractAxiom(axiomsRDD, AxiomType.OBJECT_PROPERTY_DOMAIN)
    val objectProRange = extractAxiom(axiomsRDD, AxiomType.OBJECT_PROPERTY_RANGE)
    val dataProDomain = extractAxiom(axiomsRDD, AxiomType.DATA_PROPERTY_DOMAIN)
    val dataProRange = extractAxiom(axiomsRDD, AxiomType.DATA_PROPERTY_RANGE)
    val equClass = extractAxiom(axiomsRDD, AxiomType.EQUIVALENT_CLASSES)
    var equDataProp = extractAxiom(axiomsRDD, AxiomType.EQUIVALENT_DATA_PROPERTIES)
    val equObjProp = extractAxiom(axiomsRDD, AxiomType.EQUIVALENT_OBJECT_PROPERTIES)

    // --------- extract instance elements
    var classAssertion = extractAxiom(axiomsRDD, AxiomType.CLASS_ASSERTION)
    var dataPropAssertion = extractAxiom(axiomsRDD, AxiomType.DATA_PROPERTY_ASSERTION).asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
    var objPropAssertion = extractAxiom(axiomsRDD, AxiomType.OBJECT_PROPERTY_ASSERTION).asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]

    // ---------------- Schema Rules --------------------
    // 1. we have to process owl:equivalentClass (resp. owl:equivalentProperty) before computing the transitive closure
    // of rdfs:subClassOf (resp. rdfs:sobPropertyOf)
    // O11a: (C owl:equivalentClass D) -> (C rdfs:subClassOf D )
    // O12b: (C owl:equivalentClass D) -> (D rdfs:subClassOf C )

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

    val equClassMap = equClass_Pairs
      .map(a => (a.getOperandsAsList.get(0), a.getOperandsAsList.get(1))).collect.toMap

    val equClassSwapMap =equClass_Pairs
      .map(a => (a.getOperandsAsList.get(1), a.getOperandsAsList.get(0))).collect.toMap

    // distribute the schema data structures by means of shared variables
    // Assume that the schema data is less than the instance data

    val subClassOfBC = sc.broadcast(subClassMap)
    val dataDomainMapBC = sc.broadcast(dataDomainMap)
    val objDomainMapBC = sc.broadcast(objDomainMap)
    val dataRangeMapBC = sc.broadcast(dataRangeMap)
    val objRangeMapBC = sc.broadcast(objRangeMap)
    val equClassMapBC = sc.broadcast(equClassMap)
    val equClassSwapMapBC = sc.broadcast(equClassSwapMap)

    // Compute the equivalence of classes and properties
    // O11c: (C rdfs:subClassOf D ), (D rdfs:subClassOf C ) -> (C owl:equivalentClass D)
    val eqClass = subClassOfAxiomsTrans
      .filter(a => subClassOfBC.value.contains(a.getSubClass))
      .map(a => dataFactory.getOWLEquivalentClassesAxiom(a.getSubClass, a.getSuperClass))

    // val equivClass = equClass.union(eqClass.asInstanceOf[RDD[OWLAxiom]]).distinct(parallelism)

    // for equivelantDataProperty and equivelantObjectProperty
    // O12a: (C owl:equivalentProperty D) -> (C rdfs:subPropertyOf D )
    // O12b: (C owl:equivalentProperty D) -> (D rdfs:subPropertyOf C )

    var subDProp_Pairs = equDataProp.asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .flatMap(eq => eq.asPairwiseAxioms().asScala)

    var subDProp1 = subDProp_Pairs
      .map(a => dataFactory.getOWLSubDataPropertyOfAxiom(a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))

    var subDProp2 = subDProp_Pairs
      .map(a => dataFactory.getOWLSubDataPropertyOfAxiom(a.getOperandsAsList.get(1), a.getOperandsAsList.get(0)))

    subDataProperty = sc.union(subDataProperty,
      subDProp1.asInstanceOf[RDD[OWLAxiom]],
      subDProp2.asInstanceOf[RDD[OWLAxiom]])
      .distinct(parallelism)

    //    println("\n subDataProperty closures: \n----------------\n")
    //    subDataProperty.collect().foreach(println)

    var subOProp_Pairs = equObjProp.asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .flatMap(eq => eq.asPairwiseAxioms().asScala)

    var subOProp1 = subOProp_Pairs
      .map(a => dataFactory.getOWLSubObjectPropertyOfAxiom(a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))

    var subOProp2 = subOProp_Pairs
      .map(a => dataFactory.getOWLSubObjectPropertyOfAxiom(a.getOperandsAsList.get(1), a.getOperandsAsList.get(0)))

    subObjProperty = sc.union(subObjProperty,
      subOProp1.asInstanceOf[RDD[OWLAxiom]],
      subOProp2.asInstanceOf[RDD[OWLAxiom]])
      .distinct(parallelism)

    //    println("\n subObjectProperty closures: \n----------------\n")
    //    subObjProperty.collect().foreach(println)

    // R2: (x rdfs:subPropertyOf y), (y rdfs:subPropertyOf z) -> (x rdfs:subPropertyOf z)
    // Apply R2 for OWLSubDataProperty and OWLSubObjectProperty
    val subDataPropOfAxiomsTrans = tr.computeTransitiveClosure(subDataProperty, AxiomType.SUB_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .filter(a => a.getSubProperty != a.getSuperProperty) // to exclude axioms with (C owl:subDataPropertyOf C)

    //    println("\n Transitive subDataPropOfAxiom closures: \n----------------\n")
    //    subDataPropOfAxiomsTrans.collect().foreach(println)

    val subObjPropOfAxiomsTrans = tr.computeTransitiveClosure(subObjProperty, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .filter(a => a.getSubProperty != a.getSuperProperty) // to exclude axioms with (C owl:subObjectPropertyOf C)

    //    println("\n Transitive subObjPropOfAxiom closures: \n----------------\n")
    //    subObjPropOfAxiomsTrans.collect().foreach(println)

    val subAnnPropOfAxiomsTrans = tr.computeTransitiveClosure(subAnnProp, AxiomType.SUB_ANNOTATION_PROPERTY_OF)

    val subDataPropMap = CollectionUtils
      .toMultiMap(subDataPropOfAxiomsTrans.asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    val subObjectPropMap = CollectionUtils
      .toMultiMap(subObjPropOfAxiomsTrans.asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    val subDataPropertyBC = sc.broadcast(subDataPropMap)
    val subObjectPropertyBC = sc.broadcast(subObjectPropMap)

    // O12c: (C rdfs:subPropertyOf D), (D rdfs:subPropertyOf C) -> (C owl:equivalentProperty D)
    // Apply O12c for OWLSubDataProperty and OWLSubObjectProperty
    val eqDP = subDataPropOfAxiomsTrans
      .filter(a => subDataPropertyBC.value.contains(a.getSubProperty))
      .map(a => dataFactory.getOWLEquivalentDataPropertiesAxiom(a.getSubProperty, a.getSuperProperty))

    //  val equivDP = equDataProp.union(eqDP.asInstanceOf[RDD[OWLAxiom]]).distinct(parallelism)
    //    println("\n O12c : \n----------------\n")
    //    equivDP.collect().foreach(println)

    val eqOP = subObjPropOfAxiomsTrans
      .filter(a => subObjectPropertyBC.value.contains(a.getSubProperty))
      .map(a => dataFactory.getOWLEquivalentObjectPropertiesAxiom(a.getSubProperty, a.getSuperProperty))

    // val equivOP = equObjProp.union(eqOP.asInstanceOf[RDD[OWLAxiom]]).distinct(parallelism)
    //    println("\n O12c : \n----------------\n")
    //    equivOP.collect().foreach(println)

    // Extract properties with certain OWL characteristic and broadcast
//    val functionalDataPropBC = sc.broadcast(
//      extractAxiom(axiomsRDD, AxiomType.FUNCTIONAL_DATA_PROPERTY)
//        .asInstanceOf[RDD[OWLFunctionalDataPropertyAxiom]]
//        .map(a => a.getProperty)
//        .collect())

    val functionalObjPropBC = sc.broadcast(
      extractAxiom(axiomsRDD, AxiomType.FUNCTIONAL_OBJECT_PROPERTY)
        .asInstanceOf[RDD[OWLFunctionalObjectPropertyAxiom]]
        .map(a => a.getProperty)
        .collect())

    val inversefunctionalObjPropBC = sc.broadcast(
      extractAxiom(axiomsRDD, AxiomType.INVERSE_FUNCTIONAL_OBJECT_PROPERTY)
        .asInstanceOf[RDD[OWLInverseFunctionalObjectPropertyAxiom]]
        .map(a => a.getProperty)
        .collect())

    val symmetricObjPropBC = sc.broadcast(
      extractAxiom(axiomsRDD, AxiomType.SYMMETRIC_OBJECT_PROPERTY)
        .asInstanceOf[RDD[OWLSymmetricObjectPropertyAxiom]]
        .map(a => a.getProperty)
        .collect())

    val transtiveObjPropBC = sc.broadcast(
      extractAxiom(axiomsRDD, AxiomType.TRANSITIVE_OBJECT_PROPERTY)
        .asInstanceOf[RDD[OWLTransitiveObjectPropertyAxiom]]
        .map(a => a.getProperty)
        .collect())

    val inverseObjPropBC = sc.broadcast(
      extractAxiom(axiomsRDD, AxiomType.INVERSE_OBJECT_PROPERTIES)
        .asInstanceOf[RDD[OWLInverseObjectPropertiesAxiom]]
        .map(a => (a.getFirstProperty, a.getSecondProperty))
        .collect().toMap)

    val swapInverseObjPropBC = sc.broadcast(inverseObjPropBC.value.map(_.swap))

    var allAxioms = axioms.union(subObjPropOfAxiomsTrans.asInstanceOf[RDD[OWLAxiom]])
      .union(subDataPropOfAxiomsTrans.asInstanceOf[RDD[OWLAxiom]])
      .union(subClassOfAxiomsTrans.asInstanceOf[RDD[OWLAxiom]])
      .union(subAnnPropOfAxiomsTrans)
      .union(subClassof)
      .union(subDataProperty)
      .union(subObjProperty)
      .union(eqClass.asInstanceOf[RDD[OWLAxiom]])
      .union(eqDP.asInstanceOf[RDD[OWLAxiom]])
      .union(eqOP.asInstanceOf[RDD[OWLAxiom]])
      .distinct(parallelism)

    // split ontology Axioms based on type, sameAs, and the rest of axioms
    var typeAxioms = allAxioms.filter{axiom => axiom.getAxiomType.equals(AxiomType.CLASS_ASSERTION)}
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]

    var sameAsAxioms = allAxioms.filter(axiom => axiom.getAxiomType.equals(AxiomType.SAME_INDIVIDUAL))
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]

    var SPOAxioms = allAxioms.subtract(typeAxioms.asInstanceOf[RDD[OWLAxiom]])
      .subtract(sameAsAxioms.asInstanceOf[RDD[OWLAxiom]])

    val eq: RDD[OWLClassExpression] = eqClass.map(a => a.getOperandsAsList.get(1))
    // eq.collect().foreach(println(_))

    // More OWL vocabulary used in property restrictions
    // onProperty, hasValue, allValuesFrom, someValuesFrom

    val dataHasVal = eq.filter(a => a.getClassExpressionType.equals(ClassExpressionType.DATA_HAS_VALUE))
    val objHasVal = eq.filter(a => a.getClassExpressionType.equals(ClassExpressionType.OBJECT_HAS_VALUE))
    val objAllValues = eq.filter(a => a.getClassExpressionType.equals(ClassExpressionType.OBJECT_ALL_VALUES_FROM))
    val objSomeValues = eq.filter(a => a.getClassExpressionType.equals(ClassExpressionType.OBJECT_SOME_VALUES_FROM))
//    val dataAllValues = eq.filter(a => a.getClassExpressionType.equals(ClassExpressionType.DATA_ALL_VALUES_FROM))
//    val dataSomeValues = eq.filter(a => a.getClassExpressionType.equals(ClassExpressionType.DATA_SOME_VALUES_FROM))

    val dataHasValBC = sc.broadcast(dataHasVal.asInstanceOf[RDD[OWLDataHasValue]]
      .map(a => (a.getProperty, a.getFiller)).collect().toMap)

    val objHasValBC = sc.broadcast(objHasVal.asInstanceOf[RDD[OWLObjectHasValue]]
      .map(a => (a.getProperty, a.getFiller)).collect().toMap)

    val objAllValuesBC = sc.broadcast(objAllValues.asInstanceOf[RDD[OWLObjectAllValuesFrom]]
      .map(a => (a.getProperty, a.getFiller)).collect().toMap)

    val objAllValuesBCSwapBC = sc.broadcast(objAllValues.asInstanceOf[RDD[OWLObjectAllValuesFrom]]
      .map(a => (a.getFiller, a.getProperty)).collect().toMap)

    val objSomeValuesBC = sc.broadcast(objSomeValues.asInstanceOf[RDD[OWLObjectSomeValuesFrom]]
      .map(a => (a.getProperty, a.getFiller)).collect().toMap)

    val objSomeValuesSwapBC = sc.broadcast(objSomeValues.asInstanceOf[RDD[OWLObjectSomeValuesFrom]]
      .map(a => (a.getFiller, a.getProperty)).collect().toMap)

//    val dataAllValuesBC = sc.broadcast(dataAllValues.asInstanceOf[RDD[OWLDataAllValuesFrom]]
//      .map(a => (a.getProperty, a.getFiller)).collect().toMap)

//    val dataSomeValuesBC = sc.broadcast(dataSomeValues.asInstanceOf[RDD[OWLDataSomeValuesFrom]]
//      .map(a => (a.getProperty, a.getFiller)).collect().toMap)

    //    val dataSomeValuesSwapBC = sc.broadcast(dataSomeValues.asInstanceOf[RDD[OWLDataSomeValuesFrom]]
    //      .map(a => (a.getFiller, a.getProperty)).collect().toMap)

    //    val ca = CollectionUtils
    //      .toMultiMap(typeAxioms.map(a => (a.getClassExpression, a.getIndividual)).collect())
    //    val caBC = sc.broadcast(ca)
    //
    //    val e = equClass.asInstanceOf[RDD[OWLEquivalentClassesAxiom]]
    //      .filter(a => caBC.value.exists(n => a.contains(n._1)) && a.getOperandsAsList.get(1).isInstanceOf[OWLDataHasValue])



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

     // O7a: (P owl:inverseOf Q), (X P Y) -> (Y Q X)
     val O7a = objPropAssertion
       .filter(a => inverseObjPropBC.value.contains(a.getProperty))
       .map(a => dataFactory.getOWLObjectPropertyAssertionAxiom(inverseObjPropBC.value(a.getProperty), a.getObject, a.getSubject))

     // O7b: (P owl:inverseOf Q), (X Q Y) -> (Y P X)
     val O7b = objPropAssertion
       .filter(a => swapInverseObjPropBC.value.contains(a.getProperty))
       .map(a => dataFactory.getOWLObjectPropertyAssertionAxiom(swapInverseObjPropBC.value(a.getProperty), a.getObject, a.getSubject))

     dataPropAssertion = dataPropAssertion.union(R3a)
     objPropAssertion = objPropAssertion.union(O3).union(R3b).union(O7a).union(O7b)
     //  objPropAssertion.collect.foreach(println)

     var newAxioms = new UnionRDD(sc, Seq(R3a.asInstanceOf[RDD[OWLAxiom]], objPropAssertion.asInstanceOf[RDD[OWLAxiom]]))
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

     val R4b = objPropAssertion // .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
       .filter(a => objDomainMapBC.value.contains(a.getProperty))
       .map(a => dataFactory.getOWLClassAssertionAxiom(objDomainMapBC.value(a.getProperty), a.getSubject))
       .setName("R4b")

     // R5: a rdfs:range x . y a z  -->  z rdf:type x .

     val R5a = dataPropAssertion.filter(a => dataRangeMapBC.value.contains(a.getProperty) && !a.getObject.isLiteral) // Add checking for non-literals
       .map(a => dataFactory.getOWLClassAssertionAxiom
     (dataRangeMapBC.value(a.getProperty).asInstanceOf[OWLClassExpression], a.getObject.asInstanceOf[OWLIndividual]))
       .setName("R5a")

     val R5b = objPropAssertion // .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
       .filter(a => objRangeMapBC.value.contains(a.getProperty))
       .map(a => dataFactory.getOWLClassAssertionAxiom(objRangeMapBC.value(a.getProperty), a.getObject))
       .setName("R5b")

     // 4. SubClass inheritance according to rdfs9
     // R6: x rdfs:subClassOf y . z rdf:type x -->  z rdf:type y .
     val R6 = typeAxioms
       .filter(a => subClassOfBC.value.contains(a.getClassExpression))
       .flatMap(a => subClassOfBC.value(a.getClassExpression).map(s => dataFactory.getOWLClassAssertionAxiom(s, a.getIndividual)))
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

     val sd = subOperands.filter(s => s._2.isInstanceOf[OWLDataHasValue])
       .map(s => (s._1, s._2.asInstanceOf[OWLDataHasValue]))

     val O14_data_b = typeAxioms.filter(a => subClassOfBC.value.contains(a.getClassExpression))
       .map(a => (a.getClassExpression, a.getIndividual))
       .join(sd).filter(x => x._2._2.isInstanceOf[OWLDataHasValue])
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

     val e_swap: RDD[(OWLDataPropertyExpression, OWLClassExpression)] = e.map(a => (a._2.getProperty, a._1))
     val O13_data_a = dataPropAssertion.filter(a => dataHasValBC.value.contains(a.getProperty))
       .map(a => (a.getProperty, a.getSubject))
       .join(e_swap)
       .map(a => dataFactory.getOWLClassAssertionAxiom(a._2._2, a._2._1))

     // case b: OWLSubClassOf(S, OWLDataHasValue(P, w)) , OWLClassAssertion(S, i)
     val s_swap = sd.map(a => (a._2.getProperty, a._1))
     val O13_data_b = dataPropAssertion.map(a => (a.getProperty, a.getSubject))
       .join(s_swap)
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

     // val eos: RDD[(OWLClassExpression, OWLObjectSomeValuesFrom)] = eqOperands.filter(eq => eq._2.isInstanceOf[OWLObjectSomeValuesFrom])
     //  .map(eq => (eq._1, eq._2.asInstanceOf[OWLObjectSomeValuesFrom]))
     //  val eosMapBC: Broadcast[Map[OWLClassExpression, OWLObjectSomeValuesFrom]] = sc.broadcast(eos.collect().toMap)

     //    val O15_1 = typeAxioms.filter(a => equClassMapBC.value.contains(a.getClassExpression))
     //        .map(a => (a.getClassExpression, a.getIndividual))
     //        .join(eos)
     //        .map(a => ((a._1, a._2._1), Nil))

     //    val O15_data_1: RDD[(OWLLiteral, OWLIndividual)] = dataPropAssertion.filter(a => dataSomeValuesBC.value.contains(a.getProperty))
     //      .map(a => (a.getObject, a.getSubject))

     //    val O15_data_2: RDD[(OWLIndividual, OWLClassExpression)] = typeAxioms.filter(a => dataSomeValuesBC.value.contains(a.getClassExpression)
     //      && equClassMapBC.value.contains(a.getClassExpression))
     //      .map(a => (a.getIndividual, a.getClassExpression))
     //
     //    val O15_1 = O15_data_1.join(O15_data_2)
     //      // .map(a => dataFactory.getOWLClassAssertionAxiom(a.))


     // O15: (R owl:someValuesFrom D), (R owl:onProperty P), (X P A), (A rdf:type D ) -> (X rdf:type R )
     val O15_obj_1 = objPropAssertion.filter(a => objSomeValuesBC.value.contains(a.getProperty))
       .map(a => (a.getObject, a.getSubject)) // (A, X)

     val O15_obj_2 = typeAxioms.filter(a => objSomeValuesSwapBC.value.contains(a.getClassExpression)
       && equClassMapBC.value.contains(a.getClassExpression))
       .map(a => (a.getIndividual, a.getClassExpression)) // (A, D)
     //        .join(eos)
     //        .map(a => (a._2._1, a._1))

     val O15 = O15_obj_1.join(O15_obj_2) // Join on A to get (A, (X, D))
       .map(a => dataFactory.getOWLClassAssertionAxiom(a._2._2, a._2._1))


     // O16: (R owl:allValuesFrom D), (R owl:onProperty P), (X P Y), (X rdf:type R ) -> (Y rdf:type D)
     val O16_1 = objPropAssertion.filter(a => objAllValuesBC.value.contains(a.getProperty))
       .map(a => (a.getSubject, a.getObject)) // (X, Y)

     val O16_2 = typeAxioms.filter(a => objAllValuesBCSwapBC.value.contains(a.getClassExpression)
       && equClassMapBC.value.contains(a.getClassExpression))
       .map(a => (a.getIndividual, a.getClassExpression)) // (X, R)

     val O16 = O16_1.join(O16_2).map(a => dataFactory.getOWLClassAssertionAxiom(a._2._2, a._2._1))

     val newTypeAxioms = new UnionRDD(sc, Seq(R4a, R4b, R5a, R5b, R6, O13_data_a, O13_data_b, O13_obj_a, O13_obj_b, O15, O16))
       .distinct(parallelism).subtract(typeAxioms, parallelism)

     newTypeCount = newTypeAxioms.count()

     if(newTypeCount > 0) {
       // add type axioms
       typeAxioms = typeAxioms.union(newTypeAxioms)
     }
     newData = newTypeCount > 0 || newAxiomsCount > 0


     newAxioms = sc.union(newAxioms, O14_data.asInstanceOf[RDD[OWLAxiom]], O14_obj.asInstanceOf[RDD[OWLAxiom]])
       .distinct(parallelism)
     newAxiomsCount = newAxioms.count()

     SPOAxioms = sc.union(SPOAxioms, newAxioms)
     typeAxioms = sc.union(typeAxioms, newTypeAxioms)
   }

    val infered: Long = allAxioms.count - newTypeCount - newAxiomsCount

    println("\n Finish with " + infered + " Inferred Axioms from Schema, SPO and Type rules")
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
    val O2 = O1_a.join(O1_a)      // ((P, B), (A, C))
      .filter(a => a._2._1 != a._2._2)
      .map(a => dataFactory.getOWLSameIndividualAxiom(a._2._1, a._2._2))      // A owl:sameAs C
      .distinct(parallelism)

//    println("\n O2: \n----------------\n")
//    O2.collect().foreach(println)

    var newSameAsAxioms = sc.union(O1, O2)
    sameAsAxioms = sc.union(sameAsAxioms, newSameAsAxioms).distinct(parallelism)

    var Time = System.currentTimeMillis() - startTime

    var inferedAxioms = sc.union(typeAxioms.asInstanceOf[RDD[OWLAxiom]], sameAsAxioms.asInstanceOf[RDD[OWLAxiom]], SPOAxioms)
        .subtract(axioms)

    println("\n Finish with " + inferedAxioms.count + " Inferred Axioms after adding SameAs rules")
    println("\n...finished materialization in " + (Time/1000) + " sec.")

    inferedAxioms
  }

  def extractAxiom(axiom: RDD[OWLAxiom], T: AxiomType[_]): RDD[OWLAxiom] = {
      axiom.filter(a => a.getAxiomType.equals(T))
    }
  }

 object ForwardRuleReasonerOWLHorst{

  def main(args: Array[String]): Unit = {

    val input = getClass.getResource("/ont_functional.owl").getPath

    println("=====================================")
    println("|  OWLAxioms Forward Rule Reasoner  |")
    println("=====================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrator", "net.sansa_stack.inference.spark.forwardchaining.axioms.Registrator")
      .appName("OWL Axiom Forward Chaining Rule Reasoner")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext

    // Call the functional syntax OWLAxiom builder
    val owlAxiomsRDD: OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession, input)
    //  OWLAxiomsRDD.collect().foreach(println)

    val ruleReasoner = new ForwardRuleReasonerOWLHorst(sc, 2)
    val res: RDD[OWLAxiom] = ruleReasoner(owlAxiomsRDD, input)
    // res.collect().foreach(println)

    sparkSession.stop
  }
 }
