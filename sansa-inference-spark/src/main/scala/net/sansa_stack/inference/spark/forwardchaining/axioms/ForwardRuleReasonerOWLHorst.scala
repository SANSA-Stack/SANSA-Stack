package net.sansa_stack.inference.spark.forwardchaining.axioms

import java.io.File

import scala.collection.JavaConverters._
import java.util

import net.sansa_stack.inference.spark.forwardchaining.axioms.TransitiveReasoner
import net.sansa_stack.inference.utils.CollectionUtils
import net.sansa_stack.owl.spark.rdd.{FunctionalSyntaxOWLAxiomsRDDBuilder, OWLAxiomsRDD}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._

class ForwardRuleReasonerOWLHorst (sc: SparkContext, parallelism: Int = 2) extends TransitiveReasoner{

  def this(sc: SparkContext) = this(sc, sc.defaultParallelism)

  def apply(sc: SparkContext, parallelism: Int = 2): ForwardRuleReasonerOWLHorst =
    new ForwardRuleReasonerOWLHorst(sc, parallelism)

  def apply(axioms: RDD[OWLAxiom], input: String): Unit = {

    val owlFile: File = new File(input)
    val manager = OWLManager.createOWLOntologyManager()
    val ontology: OWLOntology = manager.loadOntologyFromOntologyDocument(owlFile)
    val dataFactory = manager.getOWLDataFactory

    val axiomsRDD = axioms.cache()    // cache this RDD because it will be used quiet often


    // ------------ extract the schema elements -------------------
    val classes: RDD[OWLClass] = axiomsRDD.flatMap {
      case axiom: HasClassesInSignature => axiom.classesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

//    println("\n\nOWL Classes\n-------\n")
//    classes.collect().foreach(println)

    var subClassof = extractAxiom(axiomsRDD, AxiomType.SUBCLASS_OF)
    var subDataProperty = extractAxiom(axiomsRDD, AxiomType.SUB_DATA_PROPERTY)
    var subObjProperty = extractAxiom(axiomsRDD, AxiomType.SUB_OBJECT_PROPERTY)
    val objectProDomain = extractAxiom(axiomsRDD, AxiomType.OBJECT_PROPERTY_DOMAIN)
    val objectProRange = extractAxiom(axiomsRDD, AxiomType.OBJECT_PROPERTY_RANGE)
    val dataProDomain = extractAxiom(axiomsRDD, AxiomType.DATA_PROPERTY_DOMAIN)
    val dataProRange = extractAxiom(axiomsRDD, AxiomType.DATA_PROPERTY_RANGE)
    val equClass = extractAxiom(axiomsRDD, AxiomType.EQUIVALENT_CLASSES)
    var equDataProp = extractAxiom(axiomsRDD, AxiomType.EQUIVALENT_DATA_PROPERTIES)
    val equObjProp = extractAxiom(axiomsRDD, AxiomType.EQUIVALENT_OBJECT_PROPERTIES)

    // 1. we have to process owl:equivalentClass (resp. owl:equivalentProperty) before computing the transitive closure
    // of rdfs:subClassOf (resp. rdfs:sobPropertyOf)
    // O11a: (C owl:equivalentClass D) -> (C rdfs:subClassOf D )
    // O12b: (C owl:equivalentClass D) -> (D rdfs:subClassOf C )

    var subC1 = equClass.asInstanceOf[RDD[OWLEquivalentClassesAxiom]]
      .map(a => dataFactory.getOWLSubClassOfAxiom(a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))

    var subC2 = equClass.asInstanceOf[RDD[OWLEquivalentClassesAxiom]]
      .map(a => dataFactory.getOWLSubClassOfAxiom(a.getOperandsAsList.get(1), a.getOperandsAsList.get(0)))

    subClassof = sc.union(subClassof,
      subC1.asInstanceOf[RDD[OWLAxiom]],
      subC2.asInstanceOf[RDD[OWLAxiom]])
        .distinct(parallelism)

    // for equivelantDataProperty and equivelantObjectProperty
    // O12a: (C owl:equivalentProperty D) -> (C rdfs:subPropertyOf D )
    // O12b: (C owl:equivalentProperty D) -> (D rdfs:subPropertyOf C )

    var subDProp1 = equDataProp.asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .map(a => dataFactory.getOWLSubDataPropertyOfAxiom(a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))

    var subDProp2 = equDataProp.asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .map(a => dataFactory.getOWLSubDataPropertyOfAxiom(a.getOperandsAsList.get(1), a.getOperandsAsList.get(0)))

    subDataProperty = sc.union(subDataProperty,
      subDProp1.asInstanceOf[RDD[OWLAxiom]],
      subDProp2.asInstanceOf[RDD[OWLAxiom]])
      .distinct(parallelism)

//    println("\n subDataProperty closures: \n----------------\n")
//    subDataProperty.collect().foreach(println)

    var subOProp1 = equObjProp.asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .map(a => dataFactory.getOWLSubObjectPropertyOfAxiom(a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))

    var subOProp2 = equObjProp.asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .map(a => dataFactory.getOWLSubObjectPropertyOfAxiom(a.getOperandsAsList.get(1), a.getOperandsAsList.get(0)))

    subObjProperty = sc.union(subObjProperty,
      subOProp1.asInstanceOf[RDD[OWLAxiom]],
      subOProp2.asInstanceOf[RDD[OWLAxiom]])
      .distinct(parallelism)

//    println("\n subObjectProperty closures: \n----------------\n")
//    subObjProperty.collect().foreach(println)

    // 2. we compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf
    // R1: (x rdfs:subClassOf y), (y rdfs:subClassOf z) -> (x rdfs:subClassOf z)
    val tr = new TransitiveReasoner()

    val subClassOfAxiomsTrans = tr.computeTransitiveClosure(subClassof, AxiomType.SUBCLASS_OF)
      .asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .filter(a => a.getSubClass != a.getSuperClass)  // to exclude axioms with (C owl:subClassOf C)

//    println("\n Transitive subClassOfAxiom closures: \n----------------\n")
//    subClassOfAxiomsTrans.collect().foreach(println)

    // R2: (x rdfs:subPropertyOf y), (y rdfs:subPropertyOf z) -> (x rdfs:subPropertyOf z)
    // Apply R2 for OWLSubDataProperty and OWLSubObjectProperty
    val subDataPropOfAxiomsTrans = tr.computeTransitiveClosure(subDataProperty, AxiomType.SUB_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .filter(a => a.getSubProperty != a.getSuperProperty)  // to exclude axioms with (C owl:subDataPropertyOf C)

//    println("\n Transitive subDataPropOfAxiom closures: \n----------------\n")
//    subDataPropOfAxiomsTrans.collect().foreach(println)

    val subObjPropOfAxiomsTrans = tr.computeTransitiveClosure(subObjProperty, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .filter(a => a.getSubProperty != a.getSuperProperty)  // to exclude axioms with (C owl:subObjectPropertyOf C)

//    println("\n Transitive subObjPropOfAxiom closures: \n----------------\n")
//    subObjPropOfAxiomsTrans.collect().foreach(println)

    // Convert all RDDs into maps which should be more efficient later on
    val subClassMap = CollectionUtils
      .toMultiMap(subClassOfAxiomsTrans.asInstanceOf[RDD[OWLSubClassOfAxiom]]
        .map(a => (a.getSubClass, a.getSuperClass)).collect())

    val subDataPropMap = CollectionUtils
      .toMultiMap(subDataPropOfAxiomsTrans.asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    val subObjectPropMap = CollectionUtils
      .toMultiMap(subObjPropOfAxiomsTrans.asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    val dataDomainMap = dataProDomain.asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
      .map(a => (a.getProperty, a.getDomain)).collect().toMap

    val objDomainMap = objectProDomain.asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .map(a => (a.getProperty, a.getDomain)).collect().toMap

    val dataRangeMap = dataProRange.asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .map(a => (a.getProperty, a.getRange)).collect().toMap

    val objRangeMap = objectProRange.asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .map(a => (a.getProperty, a.getRange)).collect().toMap

    // distribute the schema data structures by means of shared variables
    // Assume that the schema data is less than the instance data

    val subClassOfBC = sc.broadcast(subClassMap)
    val subDataPropertyBC = sc.broadcast(subDataPropMap)
    val subObjectPropertyBC = sc.broadcast(subObjectPropMap)
    val dataDomainMapBC = sc.broadcast(dataDomainMap)
    val objDomainMapBC = sc.broadcast(objDomainMap)
    val dataRangeMapBC = sc.broadcast(dataRangeMap)
    val objRangeMapBC = sc.broadcast(objRangeMap)

    // Compute the equivalence of classes and properties
    // O11c: (C rdfs:subClassOf D ), (D rdfs:subClassOf C ) -> (C owl:equivalentClass D)
    val eqClass = subClassOfAxiomsTrans
      .filter(a => subClassOfBC.value.contains(a.getSubClass))
      .map(a => dataFactory.getOWLEquivalentClassesAxiom(a.getSubClass, a.getSuperClass))

    val equivClass = equClass.union(eqClass.asInstanceOf[RDD[OWLAxiom]]).distinct(parallelism)

    // O12c: (C rdfs:subPropertyOf D), (D rdfs:subPropertyOf C) -> (C owl:equivalentProperty D)
    // Apply O12c for OWLSubDataProperty and OWLSubObjectProperty
    val eqDP = subDataPropOfAxiomsTrans
      .filter(a => subDataPropertyBC.value.contains(a.getSubProperty))
      .map(a => dataFactory.getOWLEquivalentDataPropertiesAxiom(a.getSubProperty, a.getSuperProperty))

    val equivDP = equDataProp.union(eqDP.asInstanceOf[RDD[OWLAxiom]]).distinct(parallelism)
//    println("\n O12c : \n----------------\n")
//    equivDP.collect().foreach(println)

    val eqOP = subObjPropOfAxiomsTrans
      .filter(a => subObjectPropertyBC.value.contains(a.getSubProperty))
      .map(a => dataFactory.getOWLEquivalentObjectPropertiesAxiom(a.getSubProperty, a.getSuperProperty))

    val equivOP = equObjProp.union(eqOP.asInstanceOf[RDD[OWLAxiom]]).distinct(parallelism)
//    println("\n O12c : \n----------------\n")
//    equivOP.collect().foreach(println)

    // Extract properties with certain OWL characteristic and broadcast
    val functionalDataPropBC = sc.broadcast(
      extractAxiom(axiomsRDD, AxiomType.FUNCTIONAL_DATA_PROPERTY)
        .asInstanceOf[RDD[OWLFunctionalDataPropertyAxiom]]
        .map(a => a.getProperty)
        .collect())

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

    // val swapInverseObjPropBC = sc.broadcast(inverseObjPropBC.value.map(_.swap))

  }

  def extractAxiom(axiom: RDD[OWLAxiom], T: AxiomType[_]): RDD[OWLAxiom] = {
    axiom.filter(a => a.getAxiomType.equals(T))
  }
}

object ForwardRuleReasonerOWLHorst{

  def main(args: Array[String]): Unit = {

    val input = "/home/heba/SANSA_Inference/SANSA-Inference/sansa-inference-spark/src/main/resources/ont_functional.owl"

    println("=====================================")
    println("|  OWLAxioms Forward Rule Reasoner  |")
    println("=====================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrator", "net.sansa_stack.inference.spark.forwardchaining.axioms.Registrator")
      .appName("OWLAxioms Forward Rule Reasoner")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext

    // Call the functional syntax OWLAxiom builder

    var OWLAxiomsRDD: OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession, input)
     OWLAxiomsRDD.collect().foreach(println)

    val RuleReasoner: Unit = new ForwardRuleReasonerOWLHorst(sc, 2).apply(OWLAxiomsRDD, input)

    sparkSession.stop
  }
}