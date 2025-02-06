package net.sansa_stack.inference.spark.forwardchaining.axioms

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.apibinding.OWLManager

import net.sansa_stack.inference.utils.{CollectionUtils, Logging}
import net.sansa_stack.owl.spark.owlAxioms
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder


/**
  * A forward chaining implementation for the RDFS entailment regime that works
  * on OWL axioms.
  *
  * Entailment pattern naming taken from
  * https://www.w3.org/TR/rdf11-mt/#patterns-of-rdfs-entailment-informative
  *
  * @param sc The Apache Spark context
  * @param parallelism The degree of parallelism
  * @author Heba Mohamed
  */

class ForwardRuleReasonerRDFS(sc: SparkContext, parallelism: Int = 2) extends Logging {

  def apply(sc: SparkContext, parallelism: Int = 2): ForwardRuleReasonerRDFS =
    new ForwardRuleReasonerRDFS(sc, parallelism)


  def apply(axioms: RDD[OWLAxiom]): RDD[OWLAxiom] = {

    val manager = OWLManager.createOWLOntologyManager()
    val dataFactory = manager.getOWLDataFactory
    val axiomsRDD = axioms.cache()    // cache this RDD because it will be used quiet often

    // ------------ extract the schema data -------------------
    // Schema classes
    val classAsserAxiom = owlAxioms.extractAxioms(axiomsRDD, AxiomType.CLASS_ASSERTION)
                                   .asInstanceOf[RDD[OWLClassAssertionAxiom]].cache()
    
    val subClassOfAxiom = owlAxioms.extractAxioms(axiomsRDD, AxiomType.SUBCLASS_OF)
    val subDataPropertyOfAxiom = owlAxioms.extractAxioms(axiomsRDD, AxiomType.SUB_DATA_PROPERTY)
    var subObjectPropertyOfAxiom = owlAxioms.extractAxioms(axiomsRDD, AxiomType.SUB_OBJECT_PROPERTY)
    val objectProDomain = owlAxioms.extractAxioms(axiomsRDD, AxiomType.OBJECT_PROPERTY_DOMAIN)
    val dataProDomain = owlAxioms.extractAxioms(axiomsRDD, AxiomType.DATA_PROPERTY_DOMAIN)
    val AnnProDomain = owlAxioms.extractAxioms(axiomsRDD, AxiomType.ANNOTATION_PROPERTY_DOMAIN)
    val dataProRange = owlAxioms.extractAxioms(axiomsRDD, AxiomType.DATA_PROPERTY_RANGE)
    val objProRange = owlAxioms.extractAxioms(axiomsRDD, AxiomType.OBJECT_PROPERTY_RANGE)
    val dataPropAsserAxiom = owlAxioms.extractAxioms(axiomsRDD, AxiomType.DATA_PROPERTY_ASSERTION)
    val objPropAsserAxiom = owlAxioms.extractAxioms(axiomsRDD, AxiomType.OBJECT_PROPERTY_ASSERTION)
    val AnnAsserAxiom = owlAxioms.extractAxioms(axiomsRDD, AxiomType.ANNOTATION_ASSERTION)
    val subAnnProp = owlAxioms.extractAxioms(axiomsRDD, AxiomType.SUB_ANNOTATION_PROPERTY_OF)

   // start to calculate transitive rules

    /** OWL Horst rules:
      * rule 1
      *
      * rdfs11 x rdfs:subClassOf y .
      *        y rdfs:subClassOf z . x rdfs:subClassOf z .
    */

//    val subClassOf = subClassOfAxiom.asInstanceOf[RDD[OWLsubClassOfAxiom]]
//    val op1 = subClassOf.map { a => (a.getSubClass, a.getSuperClass) }
//    val op2 = subClassOf.map { a => (a.getSuperClass, a.getSubClass) }
//    val subClass_ExtVP_OS = op2.join(op1).map(a => (a._1, a._2._1)).distinct()
//    val subClass_ExtVP_SO = op1.join(op2).map(a => (a._1, a._2._1)).distinct()
//
//    var startTime = System.currentTimeMillis()
//    val Trans = subClass_ExtVP_OS.join(subClass_ExtVP_SO).map(a => (a._2._1, a._2._2))
//    var endTime = System.currentTimeMillis() - startTime
//    println("\n...first --- " + (endTime) + " milisec.")
//
//    val res = Trans.map(a => dataFactory.getOWLsubClassOfAxiom(a._1, a._2)).distinct
//
//    println("\n ---- Trans ----\n")
//    Trans.collect.foreach(println(_))
//    startTime = System.currentTimeMillis()

    val tr = new TransitiveReasoner()
    val subClassOfAxiomsTrans = tr.computeTransitiveClosure(subClassOfAxiom, AxiomType.SUBCLASS_OF)
      .setName("rdfs11")

    /* rule 2 --> rule 2a, rule 2b
       rdfs5 x rdfs:subPropertyOf y .
             y rdfs:subPropertyOf z ->  x rdfs:subPropertyOf z .
       to calculate rdf5 we need to get subDataProperty and subObjectProperty
    */

    val subDataPropertyOfAxiomsTrans = tr.computeTransitiveClosure(subDataPropertyOfAxiom, AxiomType.SUB_DATA_PROPERTY)
      .setName("rdfs5a")

    val subObjectPropertyOfAxiomsTrans = tr.computeTransitiveClosure(subObjectPropertyOfAxiom, AxiomType.SUB_OBJECT_PROPERTY)
      .setName("rdfs5b")

    val subAnnotationPropertyOfAxiomsTrans = tr.computeTransitiveClosure(subAnnProp, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
      .setName("rdfs5c")

    var allAxioms = axioms.union(subObjectPropertyOfAxiomsTrans)
      .union(subDataPropertyOfAxiomsTrans.asInstanceOf[RDD[OWLAxiom]])
      .union(subClassOfAxiomsTrans)
      .union(subAnnotationPropertyOfAxiomsTrans)
      .distinct()

    val subClassMap = CollectionUtils
      .toMultiMap(subClassOfAxiomsTrans.asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .map(a => (a.getSubClass, a.getSuperClass)).collect())

    val subDataPropMap = CollectionUtils
      .toMultiMap(subDataPropertyOfAxiomsTrans.asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    val subObjectPropMap = CollectionUtils
      .toMultiMap(subObjectPropertyOfAxiomsTrans.asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    val subAnnPropMap = CollectionUtils
      .toMultiMap(subAnnotationPropertyOfAxiomsTrans.asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
        .map(a => (a.getSubProperty, a.getSuperProperty)).collect())

    // distribute the schema data structures by means of shared variables
    val subClassOfBC = sc.broadcast(subClassMap)
    val subDataPropertyBC = sc.broadcast(subDataPropMap)
    val subObjectPropertyBC = sc.broadcast(subObjectPropMap)
    val subAnnPropertyBC = sc.broadcast(subAnnPropMap)

    // split ontology Axioms based on type, sameAs, and the rest of axioms

    var typeAxioms = classAsserAxiom.asInstanceOf[RDD[OWLAxiom]]
    var sameAsAxioms = owlAxioms.extractAxioms(axiomsRDD, AxiomType.SAME_INDIVIDUAL)
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

    val RDFS7b = objPropAsserAxiom.asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => subObjectPropertyBC.value.contains(a.getProperty))
      .flatMap(a => subObjectPropertyBC.value(a.getProperty)
        .map(s => dataFactory.getOWLObjectPropertyAssertionAxiom(s, a.getSubject, a.getObject)))
      .setName("rdfs7b")

    val RDFS7c = AnnAsserAxiom.asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]
        .filter(a => subAnnPropertyBC.value.contains(a.getProperty))
        .flatMap(a => subAnnPropertyBC.value(a.getProperty)
        .map(s => dataFactory.getOWLAnnotationAssertionAxiom(s, a.getSubject, a.getValue)))
      .setName("rdfs7c")

    SPOAxioms = sc.union(SPOAxioms,
      RDFS7a.asInstanceOf[RDD[OWLAxiom]],
      RDFS7b.asInstanceOf[RDD[OWLAxiom]],
      RDFS7c.asInstanceOf[RDD[OWLAxiom]])
     .setName("SPO Axioms + rule 7a + rule 7b + rule 7c ")

    /* 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

        rule 4: --> rule 4a, rule 4b

    rdfs2:  a rdfs:domain b . x a y  -->  x rdf:type b .
     */

   val dataDomainMap = dataProDomain.asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
     .map(a => (a.getProperty, a.getDomain)).collect().toMap

   val dataDomainMapBC = sc.broadcast(dataDomainMap)

   val RDFS2a = dataPropAsserAxiom.asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(a => dataDomainMapBC.value.contains(a.getProperty))
      .map(a => dataFactory.getOWLClassAssertionAxiom(dataDomainMapBC.value(a.getProperty), a.getSubject))
      .setName("rdfs2a")

   val objDomainMap = objectProDomain.asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .map(a => (a.getProperty, a.getDomain)).collect().toMap

   val objDomainMapBC = sc.broadcast(objDomainMap)

   val RDFS2b = objPropAsserAxiom.asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => objDomainMapBC.value.contains(a.getProperty))
      .map(a => dataFactory.getOWLClassAssertionAxiom(objDomainMapBC.value(a.getProperty), a.getSubject))
      .setName("rdfs2b")

   val AnnDomainMap = AnnProDomain.asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .map(a => (a.getProperty, a.getDomain)).collect().toMap

   val AnnDomainMapBC = sc.broadcast(AnnDomainMap)

   val RDFS2c = AnnAsserAxiom.asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]
      .filter(a => AnnDomainMapBC.value.contains(a.getProperty))
      .map(a => dataFactory
        .getOWLClassAssertionAxiom(dataFactory.getOWLClass(AnnDomainMapBC.value(a.getProperty)),
          dataFactory.getOWLNamedIndividual(a.getSubject.toString)))
      .setName("rdfs2c")

    /* rule 5: --> rule 5a, rule 5b

          rdfs3: a rdfs:range x . y a z  -->  z rdf:type x .
     */

    val dataRangeMap = dataProRange.asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .map(a => (a.getProperty, a.getRange)).collect().toMap

    val dataRangeMapBC = sc.broadcast(dataRangeMap)

    val RDFS3a = dataPropAsserAxiom.asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(a => dataRangeMapBC.value.contains(a.getProperty) && !a.getObject.isLiteral)  // Add checking for non-literals
      .map(a => dataFactory.getOWLClassAssertionAxiom
              (dataRangeMapBC.value(a.getProperty).asInstanceOf[OWLClassExpression],
                a.getObject.asInstanceOf[OWLIndividual]))
      .setName("rdfs3a")

    val objRangeMap = objProRange.asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .map(a => (a.getProperty, a.getRange)).collect().toMap

    val objRangeMapBC = sc.broadcast(objRangeMap)

    val RDFS3b = objPropAsserAxiom.asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => objRangeMapBC.value.contains(a.getProperty))  // Add checking for non-literals
      .map(a => dataFactory.getOWLClassAssertionAxiom(objRangeMapBC.value(a.getProperty), a.getObject))
      .setName("rdfs3b")

    // rdfs2 and rdf3 generate classAssertionAxiom which we will add to typeAxioms
    val axiome23abc = sc.union(RDFS2a, RDFS2b, RDFS2c, RDFS3a, RDFS3b)
      .distinct(parallelism).asInstanceOf[RDD[OWLAxiom]]
      .setName("rdfs2a + rdfs2b + rdfs2c+ rdfs3a + rdfs3b")

    typeAxioms = typeAxioms.union(axiome23abc).distinct.setName("classAssertion + rdfs2abc + rdfs3ab")

    // 4. SubClass inheritance according to rdfs9
    /*   rule 6

       rdfs9: x rdfs:subClassOf y . z rdf:type x -->  z rdf:type y .
    */

    val RDFs9 = typeAxioms.asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .filter(a => subClassOfBC.value.contains(a.getClassExpression))
      .flatMap(a => subClassOfBC.value(a.getClassExpression)
        .map(s => dataFactory.getOWLClassAssertionAxiom(s, a.getIndividual)))
      .setName("rdfs9")

    typeAxioms = typeAxioms.union(RDFs9.asInstanceOf[RDD[OWLAxiom]])

    // merge all the resulting axioms
    allAxioms = sc.union(Seq(SPOAxioms, typeAxioms, sameAsAxioms))
        .distinct(parallelism)
        .setName("typeAxioms + sameAsAxioms + SPOAxioms")

    val inferred = allAxioms.subtract(axioms)
    val inferredCount = inferred.count()

    log.info(s"Finished with $inferredCount inferred axioms")

    allAxioms
 }
}

// object ForwardRuleReasonerRDFS{
//
//  def main(args: Array[String]): Unit = {
//
//    val input = getClass.getResource("/ont_functional.owl").getPath
//
//    println("=====================================")
//    println("|  OWLAxioms Forward Rule Reasoner  |")
//    println("=====================================")
//
//    val sparkSession = SparkSession.builder
//      .master("local[*]")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .appName("OWL Axioms Forward Rule Reasoner")
//      .getOrCreate()
//
//    val sc: SparkContext = sparkSession.sparkContext
//
//    // Call the functional syntax OWLAxiom builder
//
//    var owlAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession, input)
//    val ruleReasoner = new ForwardRuleReasonerRDFS(sc, 2)
//    val res: RDD[OWLAxiom] = ruleReasoner(owlAxiomsRDD)
//
//    sparkSession.stop
//  }
// }
