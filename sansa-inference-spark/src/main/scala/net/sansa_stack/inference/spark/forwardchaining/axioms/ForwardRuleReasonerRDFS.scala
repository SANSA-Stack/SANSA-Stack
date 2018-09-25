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

    val dataFactory = manager.getOWLDataFactory

    val axiomsRDD = axioms.cache()    // cache this RDD because it will be used quiet often

    // ------------ extract the schema data -------------------
    // Schema classes

    val classes: RDD[OWLClass] = axiomsRDD.flatMap {
      case axiom: HasClassesInSignature => axiom.classesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    println("\n\nOWL Classes\n-------\n")
    classes.collect().foreach(println)
    // classes.take(classes.count().toInt).foreach(println(_))

    // OWLClassAssertionAxiom
    val classAsserAxiom = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.CLASS_ASSERTION)).asInstanceOf[RDD[OWLClassAssertionAxiom]].cache()

//    val cmap: Map[OWLClassExpression, Set[OWLIndividual]] = CollectionUtils
//      .toMultiMap(classAsserAxiom.asInstanceOf[RDD[OWLClassAssertionAxiom]]
//        .map(a => (a.getClassExpression, a.getIndividual)).collect())
//    val c: Broadcast[Map[OWLClassExpression, Set[OWLIndividual]]] = sc.broadcast(cmap)

//    val r = new Reasoner(axioms)
//    val b: RDD[OWLClassAssertionAxiom] = classAsserAxiom.filter(a => r.isEntailed(a))
//    println("\n\nb\n-------\n")
//
//    b.take(b.count().toInt).foreach(println(_))

    println("\n\nOWLClassAssertionAxioms\n-------\n")
    classAsserAxiom.collect().foreach(println)

    // OWLSubClassofAxiom
    val subClassofAxiom: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.SUBCLASS_OF))

    println("\n\nOWLSubClassofAxioms\n-------\n")
    subClassofAxiom.collect().foreach(println)

    // OWLSubDataPropertyofAxiom
    val subDataPropertyofAxiom = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.SUB_DATA_PROPERTY))
        // .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
    println("\n\nOWLSubDataPropertyofAxioms\n-------\n")
    subDataPropertyofAxiom.collect().foreach(println)

    // OWLSubObjectPropertyofAxiom
    val subObjectPropertyofAxiom: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.SUB_OBJECT_PROPERTY))
    println("\n\nOWLSubObjectPropertyofAxioms\n-------\n")
    subObjectPropertyofAxiom.collect().foreach(println)

    // OWLObjectPropertyDomainAxiom
    val objectProDomain: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.OBJECT_PROPERTY_DOMAIN))

    println("\n\nOWLObjectPropertyDomainAxiom\n-------\n")
    objectProDomain.collect().foreach(println)

    // OWLDataPropertyDomainAxiom
    val dataProDomain: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.DATA_PROPERTY_DOMAIN))

    println("\n\nOWLDataPropertyDomainAxiom\n-------\n")
    dataProDomain.collect().foreach(println)

    // OWLDataPropertyRangeAxiom
    val dataProRange: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.DATA_PROPERTY_RANGE))

    println("\n\nOWLDataPropertyRangeAxiom\n-------\n")
    dataProRange.collect().foreach(println)

    // OWLObjectPropertyRangeAxiom
    val objProRange: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.OBJECT_PROPERTY_RANGE))

    println("\n\nOWLObjectPropertyRangeAxiom\n-------\n")
    objProRange.collect().foreach(println)

    // OWLDataPropertyAssertionAxiom
    val dataPropAsserAxiom: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.DATA_PROPERTY_ASSERTION))

    println("\n\nOWLDataPropertyAssertionAxiom\n-------\n")
    dataPropAsserAxiom.collect().foreach(println)

    // OWLObjectPropertyAssertionAxiom
    val objPropAsserAxiom: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.OBJECT_PROPERTY_ASSERTION))

    println("\n\nOWLObjectPropertyAssertionAxiom\n-------\n")
    objPropAsserAxiom.collect().foreach(println)

    // OWLObjectPropertyAssertionAxiom
    val subAnnProp: RDD[OWLAxiom] = axiomsRDD
      .filter(axiom => axiom.getAxiomType.equals(AxiomType.SUB_ANNOTATION_PROPERTY_OF))

    println("\n\nOWLSubAnnotationPropertyOAxiom\n-------\n")
    subAnnProp.collect().foreach(println)

    // OWLIndividuals
    val individuals : RDD[OWLNamedIndividual] = axiomsRDD.flatMap {
      case axiom : HasIndividualsInSignature => axiom.individualsInSignature().collect(Collectors.toSet()).asScala
      case _ => null
    }.filter(_ != null).distinct()

    println("\nIndividuals\n-----------")
    individuals.collect().foreach(println)

    // OWL Data Property
    val dataProperty = axiomsRDD.flatMap{
      case axiom : HasDataPropertiesInSignature => axiom.dataPropertiesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    println("\n Data properties: \n----------------\n")
    dataProperty.collect().foreach(println)

   //  annotated properties
   val annotatedProperties: RDD[OWLAnnotationProperty] = axiomsRDD.flatMap{
      case axiom : HasAnnotationPropertiesInSignature => axiom.annotationPropertiesInSignature.iterator().asScala
      case _ => null
   }.filter(_ != null).distinct()

    println("\n Annotated properties: \n----------------\n")
    annotatedProperties.collect().foreach(println)

   // start to calculate transitive rules

    /** OWL Horst rules:
      * rule 1
      *
      * rdfs11 x rdfs:subClassOf y .
      *        y rdfs:subClassOf z . x rdfs:subClassOf z .
    */

    val tr = new TransitiveReasoner()
    val subClassOfAxiomsTrans = tr.computeTransitiveClosure(subClassofAxiom, AxiomType.SUBCLASS_OF).setName("rdfs11")
    // val subClassOfAxiomsTrans = tr.computeSubClassTransitiveClosure(subClassofAxiom).setName("rdfs11")

    println("\n Transitive subClassOfAxiom closures: \n----------------\n")
    subClassOfAxiomsTrans.collect().foreach(println)

    /* rule 2 --> rule 2a, rule 2b
       rdfs5 x rdfs:subPropertyOf y .
             y rdfs:subPropertyOf z ->  x rdfs:subPropertyOf z .
       to calculate rdf5 we need to get subDataProperty and subObjectProperty
    */

    // val subDataPropertyOfAxiomsTrans = tr.computeSubDataPropertyTransitiveClosure(subDataPropertyofAxiom).setName("rdfs5")
    val subDataPropertyOfAxiomsTrans = tr.computeTransitiveClosure(subDataPropertyofAxiom, AxiomType.SUB_DATA_PROPERTY).setName("rdfs5")

    println("\n Transitive subDataPropertyOfAxiom closures: \n----------------\n")
    subDataPropertyOfAxiomsTrans.collect().foreach(println)

    // val subObjectPropertyOfAxiomsTrans = tr.computeSubObjectPropertyTransitiveClosure(subObjectPropertyofAxiom).setName("rdfs5")

    val subObjectPropertyOfAxiomsTrans = tr.computeTransitiveClosure(subObjectPropertyofAxiom, AxiomType.SUB_OBJECT_PROPERTY).setName("rdfs5")

    println("\n Transitive subObjectPropertyOfAxiom closures: \n----------------\n")
    subObjectPropertyOfAxiomsTrans.collect().foreach(println)

    val subAnnotationPropertyOfAxiomsTrans = tr.computeTransitiveClosure(subAnnProp, AxiomType.SUB_ANNOTATION_PROPERTY_OF)

    println("\n Transitive subAnnotationPropertyOfAxiom closures: \n----------------\n")
    subAnnotationPropertyOfAxiomsTrans.collect().foreach(println)

    var allAxioms = axioms.union(subObjectPropertyOfAxiomsTrans)
      .union(subDataPropertyOfAxiomsTrans.asInstanceOf[RDD[OWLAxiom]])
      .union(subClassOfAxiomsTrans)
      .union(subAnnotationPropertyOfAxiomsTrans)
      .distinct()

    val subClassMap = CollectionUtils
      .toMultiMap(subClassOfAxiomsTrans.asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .map(a => (a.getSubClass, a.getSuperClass)).collect())

//    println("\nsubClassMap\n")
//    subClassMap.take(10).foreach(println(_))

    val subDataPropMap = CollectionUtils
      .toMultiMap(subDataPropertyOfAxiomsTrans.asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

//    println("\nsubDataPropMap\n")
//    subDataPropMap.take(10).foreach(println(_))

    val subObjectPropMap = CollectionUtils
      .toMultiMap(subObjectPropertyOfAxiomsTrans.asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    // distribute the schema data structures by means of shared variables
    val subClassOfBC = sc.broadcast(subClassMap)
    val subDataPropertyBC = sc.broadcast(subDataPropMap)
    val subObjectPropertyBC = sc.broadcast(subObjectPropMap)

    // split ontology Axioms based on type, sameAs, and the rest of axioms

    var typeAxioms = classAsserAxiom.asInstanceOf[RDD[OWLAxiom]]
    var sameAsAxioms = axiomsRDD.filter(axiom => axiom.getAxiomType.equals(AxiomType.SAME_INDIVIDUAL))
    var SPOAxioms = allAxioms.subtract(typeAxioms).subtract(sameAsAxioms)

    /* rule 3 --> rule 3a for subdataproperty, 3b for subobjectproperty
     * 2. SubPropertyOf inheritance according to rdfs7 is computed

     * rdfs7:   x P y .  P rdfs:subPropertyOf P1  ->     x P1 y .
     */

    val RDFS7a = dataPropAsserAxiom.asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(a => subDataPropertyBC.value.contains(a.getProperty))
      .flatMap(a => subDataPropertyBC.value(a.getProperty)
        .map(s => dataFactory.getOWLDataPropertyAssertionAxiom(s, a.getSubject, a.getObject)))
          .setName("rdfs7a")

    println("\n RDFS7a results \n----------------\n")
    RDFS7a.collect().foreach(println)

    val RDFS7b = objPropAsserAxiom.asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => subObjectPropertyBC.value.contains(a.getProperty))
      .flatMap(a => subObjectPropertyBC.value(a.getProperty)
        .map(s => dataFactory.getOWLObjectPropertyAssertionAxiom(s, a.getSubject, a.getObject)))
      .setName("rdfs7b")

    println("\n RDFS7b results \n----------------\n")
    RDFS7b.collect().foreach(println)

    SPOAxioms = SPOAxioms.union(RDFS7a.asInstanceOf[RDD[OWLAxiom]])
      .union(RDFS7b.asInstanceOf[RDD[OWLAxiom]]).setName("SPO Axioms + rule 7a + rule 7b ")

//    println("\n SPO Axioms \n----------------\n")
//    SPOAxioms.take(SPOAxioms.count().toInt).foreach(println(_))

    /* 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

        rule 4: --> rule 4a, rule 4b

    rdfs2:  a rdfs:domain b . x a y  -->  x rdf:type b .
     */

   // val dataProDomain = extractAxiom(SPOAxioms, AxiomType.DATA_PROPERTY_DOMAIN)
   val dataDomainMap: Map[OWLDataPropertyExpression, OWLClassExpression] = dataProDomain.asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
     .map(a => (a.getProperty, a.getDomain)).collect().toMap
   val dataDomainMapBC: Broadcast[Map[OWLDataPropertyExpression, OWLClassExpression]] = sc.broadcast(dataDomainMap)

    println("\ndataDomainMap\n")
    dataDomainMap.take(10).foreach(println(_))

    val RDFS2a = dataPropAsserAxiom.asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(a => dataDomainMapBC.value.contains(a.getProperty))
      .map(a => dataFactory.getOWLClassAssertionAxiom(dataDomainMapBC.value(a.getProperty), a.getSubject))
      .setName("rdfs2a")

    println("\n RDFS2a results \n----------------\n")
    RDFS2a.collect().foreach(println)

    val objDomainMap: Map[OWLObjectPropertyExpression, OWLClassExpression] = objectProDomain.asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .map(a => (a.getProperty, a.getDomain)).collect().toMap
    val objDomainMapBC: Broadcast[Map[OWLObjectPropertyExpression, OWLClassExpression]] = sc.broadcast(objDomainMap)

    val RDFS2b = objPropAsserAxiom.asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => objDomainMapBC.value.contains(a.getProperty))
      .map(a => dataFactory.getOWLClassAssertionAxiom(objDomainMapBC.value(a.getProperty), a.getSubject))
      .setName("rdfs2b")

    println("\n RDFS2b results \n----------------\n")
    RDFS2b.collect().foreach(println)

    /* rule 5: --> rule 5a, rule 5b

          rdfs3: a rdfs:range x . y a z  -->  z rdf:type x .
     */

    val dataRangeMap: Map[OWLDataPropertyExpression, OWLDataRange] = dataProRange.asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .map(a => (a.getProperty, a.getRange)).collect().toMap
    val dataRangeMapBC: Broadcast[Map[OWLDataPropertyExpression, OWLDataRange]] = sc.broadcast(dataRangeMap)

//    println("\ndataRangeMap\n")
//    dataRangeMap.take(10).foreach(println(_))

    val RDFS3a = dataPropAsserAxiom.asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(a => dataRangeMapBC.value.contains(a.getProperty) && !a.getObject.isLiteral)  // Add checking for non-literals
      .map(a => dataFactory.getOWLClassAssertionAxiom
              (dataRangeMapBC.value(a.getProperty).asInstanceOf[OWLClassExpression], a.getObject.asInstanceOf[OWLIndividual]))
      .setName("rdfs3a")

    println("\n RDFS3a results \n----------------\n")
    RDFS3a.collect().foreach(println)

    val objRangeMap: Map[OWLObjectPropertyExpression, OWLClassExpression] = objProRange.asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .map(a => (a.getProperty, a.getRange)).collect().toMap
    val objRangeMapBC: Broadcast[Map[OWLObjectPropertyExpression, OWLClassExpression]] = sc.broadcast(objRangeMap)

//    println("\nobjRangeMap\n")
//    objRangeMap.take(10).foreach(println(_))

    val RDFS3b = objPropAsserAxiom.asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => objRangeMapBC.value.contains(a.getProperty))  // Add checking for non-literals
      .map(a => dataFactory.getOWLClassAssertionAxiom(objRangeMapBC.value(a.getProperty), a.getObject))
      .setName("rdfs3b")

    println("\n RDFS3b results \n----------------\n")
    RDFS3b.collect().foreach(println)

    // rdfs2 and rdf3 generate classAssertionAxiom which we will add to typeAxioms
    val axiome23ab = RDFS2a.union(RDFS2b).union(RDFS3a)
      .union(RDFS3b).distinct(parallelism)
      .asInstanceOf[RDD[OWLAxiom]]
      .setName("rdfs2a + rdfs2b + rdfs3a + rdfs3b")

    typeAxioms = typeAxioms.union(axiome23ab).distinct.setName("classAssertion + rdfs2ab + rdfs3ab")


    // 4. SubClass inheritance according to rdfs9
    /*   rule 6

       rdfs9: x rdfs:subClassOf y . z rdf:type x -->  z rdf:type y .
    */

//    println("\nsubClassMap\n")
//    subClassMap.take(10).foreach(println(_))
//
//    println("\ntyped axioms\n")
//    typeAxioms.take(10).foreach(println(_))

    val RDFs9 = typeAxioms.asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .filter(a => subClassOfBC.value.contains(a.getClassExpression))
      .flatMap(a => subClassOfBC.value(a.getClassExpression).map(s => dataFactory.getOWLClassAssertionAxiom(s, a.getIndividual)))
      .setName("rdfs9")

    println("\n RDFs9 results \n----------------\n")
    RDFs9.collect().foreach(println)

    typeAxioms = typeAxioms.union(RDFs9.asInstanceOf[RDD[OWLAxiom]])

    // merge all the resulting axioms
    allAxioms = sc.union(Seq(SPOAxioms, typeAxioms, sameAsAxioms))
        .distinct(parallelism)
        .setName("typeAxioms + sameAsAxioms + SPOAxioms")

//    println("\nall axioms\n")
//    allAxioms.collect().foreach(println)

  }

  def extractAxiom(axiom: RDD[OWLAxiom], T: AxiomType[_]): RDD[OWLAxiom] = {
    axiom.filter(a => a.getAxiomType.equals(T))
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
    OWLAxiomsRDD.collect().foreach(println)

    val RuleReasoner: Unit = new ForwardRuleReasonerRDFS(sc, 2).apply(OWLAxiomsRDD, input)

    sparkSession.stop
  }
}
