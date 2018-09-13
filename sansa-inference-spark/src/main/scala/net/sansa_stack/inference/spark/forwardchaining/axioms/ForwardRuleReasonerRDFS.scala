package net.sansa_stack.inference.spark.forwardchaining.axioms

import scala.collection.JavaConverters._
import java.io.File
import java.util.stream.Collectors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.apibinding.OWLManager
import net.sansa_stack.inference.utils.{CollectionUtils, Logging}
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import org.apache.jena.vocabulary.{OWL2, RDFS}
import org.apache.spark.broadcast.Broadcast


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

    val dataFactory = manager.getOWLDataFactory

   // def getDataFactory () = dataFactory

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

    // OWLSubDataPropertyofAxiom
    val subDataPropertyofAxiom = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.SUB_DATA_PROPERTY))
        .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
    println("\n\nOWLSubDataPropertyofAxioms\n-------\n")
    subDataPropertyofAxiom.take(subDataPropertyofAxiom.count().toInt).foreach(println(_))

    // OWLSubObjectPropertyofAxiom
    val subObjectPropertyofAxiom: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.SUB_OBJECT_PROPERTY))
    println("\n\nOWLSubObjectPropertyofAxioms\n-------\n")
    subObjectPropertyofAxiom.take(subObjectPropertyofAxiom.count().toInt).foreach(println(_))

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

    val dec = axiomsRDD.filter(axioms => axioms.getAxiomType.equals(AxiomType.DECLARATION))

    // println("\n Declarations: \n----------------\n")
    // dec.take(dec.count().toInt).foreach(println(_))

   // start to calculate transitive rules

    /**
      * rdfs11 x rdfs:subClassOf y .
      *        y rdfs:subClassOf z . x rdfs:subClassOf z .
    */

    val tr = new TransitiveReasoner()
    val subClassOfAxiomsTrans = tr.computeSubClassTransitiveClosure(subClassofAxiom).setName("rdfs11")

    println("\n Transitive subClassOfAxiom closures: \n----------------\n")
    subClassOfAxiomsTrans.take(10).foreach(println(_))

    /*
       rdfs5 x rdfs:subPropertyOf y .
             y rdfs:subPropertyOf z ->  x rdfs:subPropertyOf z .
       to calculate rdf5 we need to get subDataProperty and subObjectProperty
    */

    val subDataPropertyOfAxiomsTrans = tr.computeSubDataPropertyTransitiveClosure(subDataPropertyofAxiom).setName("rdfs5")

    println("\n Transitive subDataPropertyOfAxiom closures: \n----------------\n")
    subDataPropertyOfAxiomsTrans.take(10).foreach(println(_))

    val subObjectPropertyOfAxiomsTrans = tr.computeSubObjectPropertyTransitiveClosure(subObjectPropertyofAxiom).setName("rdfs5")

    println("\n Transitive subObjectPropertyOfAxiom closures: \n----------------\n")
    subObjectPropertyOfAxiomsTrans.take(10).foreach(println(_))

    var allAxioms = axioms.union(subObjectPropertyOfAxiomsTrans)
      .union(subDataPropertyOfAxiomsTrans.asInstanceOf[RDD[OWLAxiom]])
      .union(subClassOfAxiomsTrans)
      .distinct()

//    var subProperty: RDD[OWLAxiom] = subDataPropertyofAxiom
//      .union(subObjectPropertyofAxiom)
//      .distinct()
//
//    subProperty = subProperty.union(subDataPropertyOfAxiomsTrans)
//      .union(subObjectPropertyofAxiom)
//      .distinct()

    // println("\n subproperty results \n----------------\n")
    // subProperty.take(subProperty.count().toInt).foreach(println(_))


    val subClassMap: Map[OWLClassExpression, Set[OWLClassExpression]] = CollectionUtils
      .toMultiMap(subClassOfAxiomsTrans.asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .map(a => (a.getSubClass, a.getSuperClass)).collect())

    val subDataPropMap: Map[OWLDataPropertyExpression, Set[OWLDataPropertyExpression]] = CollectionUtils
      .toMultiMap(subDataPropertyOfAxiomsTrans.asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    val subObjectPropMap: Map[OWLObjectPropertyExpression, Set[OWLObjectPropertyExpression]] = CollectionUtils
      .toMultiMap(subObjectPropertyOfAxiomsTrans.asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    // distribute the schema data structures by means of shared variables
    val subClassOfBC: Broadcast[Map[OWLClassExpression, Set[OWLClassExpression]]] = sc.broadcast(subClassMap)
    val subDataPropertyBC: Broadcast[Map[OWLDataPropertyExpression, Set[OWLDataPropertyExpression]]] = sc.broadcast(subDataPropMap)
    val subObjectPropertyBC: Broadcast[Map[OWLObjectPropertyExpression, Set[OWLObjectPropertyExpression]]] = sc.broadcast(subObjectPropMap)

    // split ontology Axioms based on type, sameAs, and the rest of axioms

    val typeAxioms = classAsserAxiom
    val sameAsAxioms = axiomsRDD.filter(axiom => axiom.getAxiomType.equals(AxiomType.SAME_INDIVIDUAL))
    val SPOAxioms = allAxioms.subtract(typeAxioms).subtract(sameAsAxioms)

     // 2. SubPropertyOf inheritance according to rdfs7 is computed

    //  rdfs7:   x P y .  P rdfs:subPropertyOf P1  ->     x P1 y .

//    val RDFS7 = SPOAxioms.filter(a => subDataPropertyBC.value.contains(a))
//   //   .map(x => (x.getSubProperty, x.getSuperProperty)).map(y => dataFactory.getOWLSubDataPropertyOfAxiom(a, y._2))
//      .setName("rdfs7")
//    // .flatMap(a => subDataPropertyBC.value(a).map)
//
//   //   .flatMap(t => subPropertyMapBC.value(t.p).map(supProp => Triple.create(t.s, supProp, t.o))) // create triple (s p2 o)
//
//
//
//
//    println("\n RDFS7 results \n----------------\n")
//    RDFS7.take(RDFS7.count().toInt).foreach(println(_))
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

    var OWLAxiomsRDD: OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession, input)
    OWLAxiomsRDD.take(OWLAxiomsRDD.count().toInt).foreach(println(_))

    val reasoner = new ForwardRuleReasonerRDFS(sc, 2).apply(OWLAxiomsRDD, input)

    sparkSession.stop
  }
}
