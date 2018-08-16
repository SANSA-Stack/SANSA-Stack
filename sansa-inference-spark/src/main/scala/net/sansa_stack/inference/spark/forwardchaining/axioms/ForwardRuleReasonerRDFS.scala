package net.sansa_stack.inference.spark.forwardchaining.axioms

import scala.collection.JavaConverters._
import java.io.File
import java.util.stream.Collectors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.apibinding.OWLManager
import net.sansa_stack.inference.utils.Logging
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

import scala.collection.mutable



/**
  * A forward chaining implementation for the RDFS entailment regime that works
  * on OWL axioms
  *
  * @param sc The Apache Spark context
  * @param parallelism The degree of parallelism
  */
class ForwardRuleReasonerRDFS(sc: SparkContext, parallelism: Int = 2) extends Logging {

  def apply(sc: SparkContext, parallelism: Int = 2): ForwardRuleReasonerRDFS =
    new ForwardRuleReasonerRDFS(sc, parallelism)


  def apply(axioms: RDD[OWLAxiom], input: String): Unit = {   // : RDD[OWLAxiom]

    val owlFile: File = new File(input)

    val manager = OWLManager.createOWLOntologyManager()

    val ontology: OWLOntology = manager.loadOntologyFromOntologyDocument(owlFile)
    println("\nLoading ontology: \n----------------\n" + ontology)

    // val dataFactory = manager.getOWLDataFactory

    val axiomsRDD = axioms.cache()    // cache this RDD because it will be used quiet often

    // ------------ extract the schema data -------------------
    // Schema classes

    val classes: RDD[OWLClass] = axiomsRDD.flatMap {
      case axiom: HasClassesInSignature => axiom.classesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    println("\n\nOWL Classes\n-------\n")
    classes.take(classes.count().toInt).foreach(println(_))

    // OWLClassAssertionAxiom
    val classAsserAxiom: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.CLASS_ASSERTION))

    println("\n\nOWLClassAssertionAxioms\n-------\n")
    classAsserAxiom.take(classAsserAxiom.count().toInt).foreach(println(_))

    // OWLSubClassofAxiom
    val subClassofAxiom: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.SUBCLASS_OF))

    println("\n\nOWLSubClassofAxioms\n-------\n")
    subClassofAxiom.take(subClassofAxiom.count().toInt).foreach(println(_))

    // OWLObjectPropertyDomainAxiom
    val objectProDomain: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.OBJECT_PROPERTY_DOMAIN))

    println("\n\nOWLObjectPropertyDomainAxiom\n-------\n")
    objectProDomain.take(objectProDomain.count().toInt).foreach(println(_))

    // OWLDataPropertyDomainAxiom
    val dataProDomain: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.DATA_PROPERTY_DOMAIN))

    println("\n\nOWLDataPropertyDomainAxiom\n-------\n")
    dataProDomain.take(dataProDomain.count().toInt).foreach(println(_))

    // OWLDataPropertyAssertionAxiom
    val dataPropAsserAxiom: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.DATA_PROPERTY_ASSERTION))

    println("\n\nOWLDataPropertyAssertionAxiom\n-------\n")
    dataPropAsserAxiom.take(dataPropAsserAxiom.count().toInt).foreach(println(_))

    // OWLObjectPropertyAssertionAxiom
    val objPropAsserAxiom: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.OBJECT_PROPERTY_ASSERTION))

    println("\n\nOWLObjectPropertyAssertionAxiom\n-------\n")
    objPropAsserAxiom.take(objPropAsserAxiom.count().toInt).foreach(println(_))

    // OWLIndividuals
    val individuals : RDD[OWLNamedIndividual] = axiomsRDD.flatMap {
      case axiom : HasIndividualsInSignature => axiom.individualsInSignature().collect(Collectors.toSet()).asScala
      case _ => null
    }.filter(_ != null).distinct()

    println("\nIndividuals\n-----------")
    individuals.take(50).foreach(println(_))


    // OWL Data Property
    val dataProperty = axiomsRDD.flatMap{
      case axiom : HasDataPropertiesInSignature => axiom.dataPropertiesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    println("\n Data properties: \n----------------\n")
    dataProperty.take(10).foreach(println(_))

  // annotated properties
   val annotatedProperties: RDD[OWLAnnotationProperty] = axiomsRDD.flatMap{
      case axiom : HasAnnotationPropertiesInSignature => axiom.annotationPropertiesInSignature.iterator().asScala
      case _ => null
   }.filter(_ != null).distinct()

    println("\n Annotated properties: \n----------------\n")
    annotatedProperties.take(10).foreach(println(_))




 }
}

object ForwardRuleReasonerRDFS{

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

    val OWLAxiomsRDD: OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession, input)
     OWLAxiomsRDD.take(OWLAxiomsRDD.count().toInt).foreach(println(_))

    val reasoner = new ForwardRuleReasonerRDFS(sc, 2).apply(OWLAxiomsRDD, input)

    sparkSession.stop
  }
}