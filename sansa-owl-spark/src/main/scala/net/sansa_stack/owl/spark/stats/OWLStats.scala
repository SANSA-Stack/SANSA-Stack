package net.sansa_stack.owl.spark.stats

import org.apache.jena.vocabulary.XSD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model._
import scala.collection.JavaConverters._

import net.sansa_stack.owl.spark.owlAxioms
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder


/**
  * A Distributed implementation of OWL Statistics.
  *
  * @author Heba Mohamed
  */
class OWLStats(spark: SparkSession) extends Serializable {

  // val spark: SparkSession = SparkSession.builder().getOrCreate()

  def run (axioms: RDD[OWLAxiom]): RDD[String] = {

    val stats = UsedClassesCount(axioms, spark).Voidify()
      .union(UsedDataProperties(axioms, spark).Voidify())
      .union(UsedObjectProperties(axioms, spark).Voidify())
      .union(UsedAnnotationProperties(axioms, spark).Voidify())

    println("\n =========== OWL Statistics ===========\n")
    stats.collect().foreach(println(_))

    stats

  }

  // New Criterion
  def Classes (axioms: RDD[OWLAxiom]): RDD[OWLClass] = {
    axioms.flatMap {
      case a: HasClassesInSignature => a.classesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()
  }

  def ClassesCount (axioms: RDD[OWLAxiom]): Long = {
    axioms.flatMap {
      case a: HasClassesInSignature => a.classesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct().count()
  }

  def DataProperties (axioms: RDD[OWLAxiom]): RDD[OWLDataProperty] = {
    val dataProperties: RDD[OWLDataProperty] = axioms.flatMap {
      case a: HasDataPropertiesInSignature => a.dataPropertiesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    dataProperties
  }

  def DataPropertiesCount (axioms: RDD[OWLAxiom]): Long = {
    val dataProperties: RDD[OWLDataProperty] = axioms.flatMap {
      case a: HasDataPropertiesInSignature => a.dataPropertiesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    dataProperties.count()
  }

  def ObjectProperties (axioms: RDD[OWLAxiom]): RDD[OWLObjectProperty] = {
    val objectProperties: RDD[OWLObjectProperty] = axioms.flatMap {
      case a: HasObjectPropertiesInSignature => a.objectPropertiesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    objectProperties
  }

  def ObjectPropertiesCount (axioms: RDD[OWLAxiom]): Long = {
    val objectProperties: RDD[OWLObjectProperty] = axioms.flatMap {
      case a: HasObjectPropertiesInSignature => a.objectPropertiesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    objectProperties.count()
  }

  def ClassAssertionCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxiom(axioms, AxiomType.CLASS_ASSERTION).count()

  def SubClassCount (axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxiom(axioms, AxiomType.SUBCLASS_OF).count()

  def SubDataPropCount (axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxiom(axioms, AxiomType.SUB_DATA_PROPERTY).count()

  def SubObjectPropCount (axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxiom(axioms, AxiomType.SUB_OBJECT_PROPERTY).count()

  def SubAnnPropCount (axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxiom(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF).count()

  def DiffIndividualsCount (axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxiom(axioms, AxiomType.DIFFERENT_INDIVIDUALS).count()

  // Criterion 14.
  def Axioms (axioms: RDD[OWLAxiom]): Long = axioms.count()


  /**
    * Criterion 17. Literals
    *
    * @param axioms RDD of OWLAxioms
    * @return number of OWLAxioms that are referencing literals to subjects.
    */
  def Literals (axioms: RDD[OWLAxiom]): Long = {

    val dataPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val negativeDataPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                             .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

    val l1 = dataPropAssertion.filter(_.getObject.isLiteral).distinct().count()
    val l2 = negativeDataPropAssertion.filter(_.getObject.isLiteral).distinct().count()

    l1 + l2
  }

  /**
    * Criterion 20. Datatypes
    *
    * @param axioms RDD of axioms
    * @return histogram of types used for literals.
    */
  def Datatypes (axioms: RDD[OWLAxiom]): RDD[(IRI, Int)] = {

    val dataPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val negativeDataPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                             .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

    val l1 = dataPropAssertion.filter(a => a.getObject.isLiteral && a.getObject.getDatatype.getIRI.length() != 0)
                              .map(a => (a.getObject.getDatatype.getIRI, 1))
                              .reduceByKey(_ + _)

    val l2 = negativeDataPropAssertion.filter(a => a.getObject.isLiteral && a.getObject.getDatatype.getIRI.length() != 0)
                                      .map(a => (a.getObject.getDatatype.getIRI, 1))
                                      .reduceByKey(_ + _)

    l1.union(l2)

  }

  /**
    * Criterion 21. Languages
    *
    * @param axioms RDD of OWLAxioms
    * @return histogram of languages used for literals.
    */
  def Languages (axioms: RDD[OWLAxiom]): RDD[(String, Int)] = {

    val dataPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val negativeDataPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                             .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

    val l1 = dataPropAssertion.filter(a => a.getObject.isLiteral && !a.getObject.getLang.isEmpty)
                              .map(a => (a.getObject.getLang, 1))
                              .reduceByKey(_ + _)

    val l2 = negativeDataPropAssertion.filter(a => a.getObject.isLiteral && !a.getObject.getLang.isEmpty)
                                      .map(a => (a.getObject.getLang, 1))
                                      .reduceByKey(_ + _)
    l1.union(l2)
  }

  /**
    * Criterion 22. Average typed string length criterion.
    *
    * @param axioms RDD of OWLAxioms
    * @return the average typed string length used throughout OWL ontology.
    */
  def AvgTypedStringLength (axioms: RDD[OWLAxiom]): Double = {

    val dataPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val negativeDataPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                             .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

    val t1 = dataPropAssertion
                .filter(a => a.getObject.isLiteral && a.getObject.getDatatype.getIRI.toString.equals(XSD.xstring.getURI))
                .map(_.getObject.getLiteral.length)

    val t2 = negativeDataPropAssertion
                .filter(a => a.getObject.isLiteral && a.getObject.getDatatype.getIRI.toString.equals(XSD.xstring.getURI))
                .map(_.getObject.getLiteral.length)

    t1.union(t2).mean()
  }

  /**
    * Criterion 23. Average untyped string length criterion.
    *
    * @param axioms RDD of OWLAxioms
    * @return the average untyped string length used throughout OWL ontology.
    */
  def AvgUntypedStringLength (axioms: RDD[OWLAxiom]): Double = {

    val dataPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val negativeDataPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                             .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

    val t1 = dataPropAssertion
                  .filter(a => a.getObject.isLiteral && !a.getObject.getLang.isEmpty)
                  .map(_.getObject.getLiteral.length)

    val t2 = negativeDataPropAssertion
                  .filter(a => a.getObject.isLiteral && !a.getObject.getLang.isEmpty)
                  .map(_.getObject.getLiteral.length)

    t1.union(t2).mean()

  }

  /**
    * Criterion 24. Typed subject
    *
    * @param axioms RDD of OWLAxioms
    * @return list of typed subjects.
    */

  def TypedSubject (axioms: RDD[OWLAxiom]): RDD[String] = {

    val declarations = owlAxioms.extractAxiom(axioms, AxiomType.DECLARATION)
                                .asInstanceOf[RDD[OWLDeclarationAxiom]]
                                .map(a => a.getEntity.toString)

    val classAssertion = owlAxioms.extractAxiom(axioms, AxiomType.CLASS_ASSERTION)
                                  .asInstanceOf[RDD[OWLClassAssertionAxiom]]
                                  .map(a => a.getIndividual.toString)

    val funcDataProp = owlAxioms.extractAxiom(axioms, AxiomType.FUNCTIONAL_DATA_PROPERTY)
                                .asInstanceOf[RDD[OWLFunctionalDataPropertyAxiom]]
                                .map(a => a.getProperty.toString)

    val funcObjProp = owlAxioms.extractAxiom(axioms, AxiomType.FUNCTIONAL_OBJECT_PROPERTY)
                               .asInstanceOf[RDD[OWLFunctionalObjectPropertyAxiom]]
                               .map(a => a.getProperty.toString)

    val inverseFuncObjProp = owlAxioms.extractAxiom(axioms, AxiomType.INVERSE_FUNCTIONAL_OBJECT_PROPERTY)
                                      .asInstanceOf[RDD[OWLInverseFunctionalObjectPropertyAxiom]]
                                      .map(a => a.getProperty.toString)

    val reflexiveFuncObjProp = owlAxioms.extractAxiom(axioms, AxiomType.REFLEXIVE_OBJECT_PROPERTY)
                                        .asInstanceOf[RDD[OWLReflexiveObjectPropertyAxiom]]
                                        .map(a => a.getProperty.toString)

    val irreflexiveFuncObjProp = owlAxioms.extractAxiom(axioms, AxiomType.IRREFLEXIVE_OBJECT_PROPERTY)
                                          .asInstanceOf[RDD[OWLIrreflexiveObjectPropertyAxiom]]
                                          .map(a => a.getProperty.toString)

    val symmetricObjProp = owlAxioms.extractAxiom(axioms, AxiomType.SYMMETRIC_OBJECT_PROPERTY)
                                    .asInstanceOf[RDD[OWLSymmetricObjectPropertyAxiom]]
                                    .map(a => a.getProperty.toString)

    val aSymmetricObjProp = owlAxioms.extractAxiom(axioms, AxiomType.ASYMMETRIC_OBJECT_PROPERTY)
                                     .asInstanceOf[RDD[OWLAsymmetricObjectPropertyAxiom]]
                                     .map(a => a.getProperty.toString)

    val transitiveObjProp = owlAxioms.extractAxiom(axioms, AxiomType.TRANSITIVE_OBJECT_PROPERTY)
                                     .asInstanceOf[RDD[OWLTransitiveObjectPropertyAxiom]]
                                     .map(a => a.getProperty.toString)

    val typedSubject = spark.sparkContext
                         .union(declarations, classAssertion, funcDataProp, funcObjProp,
                                inverseFuncObjProp, reflexiveFuncObjProp, irreflexiveFuncObjProp,
                                symmetricObjProp, aSymmetricObjProp, transitiveObjProp)
                         .distinct()

    typedSubject
  }


  /**
    * Criterion 25. Labeled subjects criterion.
    *
    * @param axioms RDD of triples
    * @return list of labeled subjects.
    */
  def LabeledSubjects (axioms: RDD[OWLAxiom]): RDD[OWLAnnotationSubject] = {

    val annPropAssertion = owlAxioms.extractAxiom(axioms, AxiomType.ANNOTATION_ASSERTION)
                                    .asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]

    annPropAssertion
      .filter(a => a.getProperty.isLabel)
      .map(_.getSubject)
  }

  /**
    *  Criterion 26. SameAs axioms
    *
    * @param axioms RDD of OWLAxioms
    * @return list of SameIndividuals axioms
    */
  def SameAs (axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxiom(axioms, AxiomType.SAME_INDIVIDUAL).count()

  /**
    * Criterion 27. Links.
    *
    * Computes the frequencies of links between entities of different namespaces.
    * This measure is directed, i.e. a link from `ns1 -> ns2` is different from `ns2 -> ns1`.
    *
    * @param axioms RDD of OWLAxioms
    * @return list of namespace combinations and their frequencies.
    */

  def Links (axioms: RDD[OWLAxiom]): RDD[(String, String, Int)] = {


    val declarations = owlAxioms.extractAxiom(axioms, AxiomType.DECLARATION).asInstanceOf[RDD[OWLDeclarationAxiom]]
                        .filter(a => a.getEntity.getIRI.getNamespace != a.getEntity.getEntityType.getIRI.getNamespace)
                        .map(a => ((a.getEntity.getIRI.getNamespace, a.getEntity.getEntityType.getIRI.getNamespace), 1))

    // -------- classes --------------

    val subClasses = owlAxioms.extractAxiom(axioms, AxiomType.SUBCLASS_OF).asInstanceOf[RDD[OWLSubClassOfAxiom]]
                        .filter(a => a.getSubClass.asOWLClass().getIRI.getNamespace != a.getSuperClass.asOWLClass().getIRI.getNamespace)
                        .map(a => ((a.getSubClass.asOWLClass().getIRI.getNamespace, a.getSuperClass.asOWLClass().getIRI.getNamespace), 1))

    val disjointClasses = owlAxioms.extractAxiom(axioms, AxiomType.DISJOINT_CLASSES).asInstanceOf[RDD[OWLDisjointClassesAxiom]]
                        .filter(a => a.getOperandsAsList.get(1).asOWLClass().getIRI.getNamespace != a.getOperandsAsList.get(0).asOWLClass().getIRI.getNamespace)
                        .map(a => ((a.getOperandsAsList.get(1).asOWLClass().getIRI.getNamespace, a.getOperandsAsList.get(0).asOWLClass().getIRI.getNamespace), 1))

    // ------------- Object Properties ---------------------

    val subObjProp = owlAxioms.extractAxiom(axioms, AxiomType.SUB_OBJECT_PROPERTY).asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
                        .filter(a => a.getSubProperty.getNamedProperty.getIRI.getNamespace != a.getSuperProperty.getNamedProperty.getIRI.getNamespace)
                        .map(a => ((a.getSubProperty.getNamedProperty.getIRI.getNamespace, a.getSuperProperty.getNamedProperty.getIRI.getNamespace), 1))

    val disjointObjProp = owlAxioms.extractAxiom(axioms, AxiomType.DISJOINT_OBJECT_PROPERTIES).asInstanceOf[RDD[OWLDisjointObjectPropertiesAxiom]]
                        .filter(a => a.getOperandsAsList.get(1).getNamedProperty.getIRI.getNamespace != a.getOperandsAsList.get(0).getNamedProperty.getIRI.getNamespace)
                        .map(a => ((a.getOperandsAsList.get(1).getNamedProperty.getIRI.getNamespace, a.getOperandsAsList.get(0).getNamedProperty.getIRI.getNamespace), 1))

    val objPropDomain = owlAxioms.extractAxiom(axioms, AxiomType.OBJECT_PROPERTY_DOMAIN).asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
                          .filter(a => a.getProperty.getNamedProperty.getIRI.getNamespace != a.getDomain.asOWLClass().getIRI.getNamespace)
                          .map(a => ((a.getProperty.getNamedProperty.getIRI.getNamespace, a.getDomain.asOWLClass().getIRI.getNamespace), 1))

    val objPropRange = owlAxioms.extractAxiom(axioms, AxiomType.OBJECT_PROPERTY_RANGE).asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
                          .filter(a => a.getProperty.getNamedProperty.getIRI.getNamespace != a.getRange.asOWLClass().getIRI.getNamespace)
                          .map(a => ((a.getProperty.getNamedProperty.getIRI.getNamespace, a.getRange.asOWLClass().getIRI.getNamespace), 1))

    val eqObjectProp = owlAxioms.extractAxiom(axioms, AxiomType.EQUIVALENT_OBJECT_PROPERTIES).asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
                          .filter(a => a.getOperandsAsList.get(1).getNamedProperty.getIRI.getNamespace != a.getOperandsAsList.get(0).getNamedProperty.getIRI.getNamespace)
                          .map(a => ((a.getOperandsAsList.get(1).getNamedProperty.getIRI.getNamespace, a.getOperandsAsList.get(0).getNamedProperty.getIRI.getNamespace), 1))

    val invObjProp = owlAxioms.extractAxiom(axioms, AxiomType.INVERSE_OBJECT_PROPERTIES).asInstanceOf[RDD[OWLInverseObjectPropertiesAxiom]]
                            .filter(a => a.getFirstProperty.getNamedProperty.getIRI.getNamespace != a.getSecondProperty.getNamedProperty.getIRI.getNamespace)
                            .map(a => ((a.getFirstProperty.getNamedProperty.getIRI.getNamespace, a.getSecondProperty.getNamedProperty.getIRI.getNamespace), 1))

     // ------------- Data Properties ---------------------

    val disjointDataProp = owlAxioms.extractAxiom(axioms, AxiomType.DISJOINT_DATA_PROPERTIES).asInstanceOf[RDD[OWLDisjointDataPropertiesAxiom]]
                        .filter(a => a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace != a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace)
                        .map(a => ((a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace, a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace), 1))

    val subDataProp = owlAxioms.extractAxiom(axioms, AxiomType.SUB_DATA_PROPERTY).asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
                        .filter(a => a.getSubProperty.asOWLDataProperty().getIRI.getNamespace != a.getSuperProperty.asOWLDataProperty().getIRI.getNamespace)
                        .map(a => ((a.getSubProperty.asOWLDataProperty().getIRI.getNamespace, a.getSuperProperty.asOWLDataProperty().getIRI.getNamespace), 1))

    val dataPropDomain = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_DOMAIN).asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
                        .filter(a => a.getProperty.asOWLDataProperty().getIRI.getNamespace != a.getDomain.asOWLClass().getIRI.getNamespace)
                        .map(a => ((a.getProperty.asOWLDataProperty.getIRI.getNamespace, a.getDomain.asOWLClass().getIRI.getNamespace), 1))

    val dataPropRange = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_RANGE).asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
                        .filter(a => a.getProperty.asOWLDataProperty().getIRI.getNamespace != a.getRange.getDataRangeType.getIRI.getNamespace)
                        .map(a => ((a.getProperty.asOWLDataProperty.getIRI.getNamespace, a.getRange.getDataRangeType.getIRI.getNamespace), 1))

    val eqDataProp = owlAxioms.extractAxiom(axioms, AxiomType.EQUIVALENT_DATA_PROPERTIES).asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
                        .filter(a => a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace != a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace)
                        .map(a => ((a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace, a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace), 1))

   // ---------- Annotation properties-----------
   val subAnnProp = owlAxioms.extractAxiom(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF).asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
     .filter(a => a.getSubProperty.asOWLAnnotationProperty().getIRI.getNamespace != a.getSuperProperty.asOWLAnnotationProperty().getIRI.getNamespace)
     .map(a => ((a.getSubProperty.asOWLAnnotationProperty().getIRI.getNamespace, a.getSuperProperty.asOWLAnnotationProperty().getIRI.getNamespace), 1))

   val annPropDomain = owlAxioms.extractAxiom(axioms, AxiomType.ANNOTATION_PROPERTY_DOMAIN).asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .filter(a => a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace != a.getDomain.getNamespace)
      .map(a => ((a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace, a.getDomain.getNamespace), 1))

   val annPropRange = owlAxioms.extractAxiom(axioms, AxiomType.ANNOTATION_PROPERTY_RANGE).asInstanceOf[RDD[OWLAnnotationPropertyRangeAxiom]]
      .filter(a => a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace != a.getRange.getNamespace)
      .map(a => ((a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace, a.getRange.getNamespace), 1))

    // -------- Assertions --------------

    val sameIndv = owlAxioms.extractAxiom(axioms, AxiomType.SAME_INDIVIDUAL).asInstanceOf[RDD[OWLSameIndividualAxiom]]
                   .filter(a => a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI.getNamespace != a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI.getNamespace)
                   .map(a => ((a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI.getNamespace, a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI.getNamespace), 1))

    val diffIndv = owlAxioms.extractAxiom(axioms, AxiomType.DIFFERENT_INDIVIDUALS).asInstanceOf[RDD[OWLDifferentIndividualsAxiom]]
                  .filter(a => a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI.getNamespace != a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI.getNamespace)
                  .map(a => ((a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI.getNamespace, a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI.getNamespace), 1))

    val classAssr = owlAxioms.extractAxiom(axioms, AxiomType.CLASS_ASSERTION).asInstanceOf[RDD[OWLClassAssertionAxiom]]
                      .filter(a => a.getIndividual.asOWLNamedIndividual().getIRI.getNamespace != a.getClassExpression.asOWLClass().getIRI.getNamespace)
                      .map(a => ((a.getIndividual.asOWLNamedIndividual().getIRI.getNamespace, a.getClassExpression.asOWLClass().getIRI.getNamespace), 1))

    val objAssr = owlAxioms.extractAxiom(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION).asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
                  .filter(a => a.getSubject.asOWLNamedIndividual().getIRI.getNamespace != a.getObject.asOWLNamedIndividual().getIRI.getNamespace)
                  .map(a => ((a.getSubject.asOWLNamedIndividual().getIRI.getNamespace, a.getObject.asOWLNamedIndividual().getIRI.getNamespace), 1))

    val dataAssr = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_ASSERTION).asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
                  .filter(a => a.getSubject.asOWLNamedIndividual().getIRI.getNamespace != a.getObject.getDatatype.getIRI.getNamespace)
                  .map(a => ((a.getSubject.asOWLNamedIndividual().getIRI.getNamespace, a.getObject.getDatatype.getIRI.getNamespace), 1))

    val links = spark.sparkContext.union(declarations, subClasses, disjointClasses, disjointObjProp, disjointDataProp, subObjProp,
                             subDataProp, subAnnProp, objPropDomain, dataPropDomain, annPropDomain, objPropRange, dataPropRange,
                             annPropRange, eqObjectProp, eqDataProp, invObjProp, sameIndv, diffIndv, classAssr, objAssr, dataAssr)
                     .reduceByKey(_ + _)
                     .map(a => (a._1._1, a._1._2, a._2))


    links
  }

  /**
    * 28.Maximum value per property {int,float,double} criterion
    *
    * @param axioms RDD of OWLAxioms
    * @return entities with their maximum values
    */
  def MaxPerProperty(axioms: RDD[OWLAxiom]): RDD[(OWLDataPropertyExpression, Int)] = {

    val dataPropAssr = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
                                .filter(a => a.getObject.isLiteral &&
                                  (a.getObject.getDatatype.isInteger ||
                                   a.getObject.getDatatype.isDouble ||
                                   a.getObject.getDatatype.isFloat))
                                .map(a => (a.getProperty, a.getObject.getLiteral.toInt))

    val negDataPropAssr = owlAxioms.extractAxiom(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                   .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]
                                   .filter(a => a.getObject.isLiteral &&
                                      (a.getObject.getDatatype.isInteger ||
                                       a.getObject.getDatatype.isDouble ||
                                       a.getObject.getDatatype.isFloat))
                                   .map(a => (a.getProperty, a.getObject.getLiteral.toInt))

    val max = dataPropAssr.union(negDataPropAssr)
                          .reduceByKey(_ max _)

    max
  }


  /**
    * Criterion 29. Average value per numeric property {int,float,double}
    *
    * @param axioms RDD of OWLAxioms
    * @return properties with their average values
    */

  def AvgPerProperty(axioms: RDD[OWLAxiom]): RDD[(OWLDataPropertyExpression, Double)] = {

    val dataPropAssr = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
                                     .filter(a => a.getObject.isLiteral &&
                                                 (a.getObject.getDatatype.isInteger ||
                                                  a.getObject.getDatatype.isDouble ||
                                                  a.getObject.getDatatype.isFloat))
                                     .map(a => (a.getProperty, a.getObject))

    val negDataPropAssr = owlAxioms.extractAxiom(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                   .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]
                                   .filter(a => a.getObject.isLiteral &&
                                                (a.getObject.getDatatype.isInteger ||
                                                 a.getObject.getDatatype.isDouble ||
                                                 a.getObject.getDatatype.isFloat))
                                   .map(a => (a.getProperty, a.getObject))

    val average = dataPropAssr.union(negDataPropAssr)
                    .aggregateByKey((0.0, 0))(
                        (a, l) => (a._1 + l.getLiteral.toDouble, a._2 + 1),
                        (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
                    .map(e => (e._1, e._2._1 / e._2._2))

    average

  }
}

// Criterion 1.
class UsedClasses (axioms: RDD[OWLAxiom], spark: SparkSession) {

  // ?p=rdf:type && isIRI(?o)
  // ClassAssertions (C, indv)
  def Filter(): RDD[OWLClassAssertionAxiom] = owlAxioms.extractAxiom(axioms, AxiomType.CLASS_ASSERTION)
                                                       .asInstanceOf[RDD[OWLClassAssertionAxiom]]

  // M[?o]++
  def Action(): RDD[OWLClassExpression] = Filter().map(_.getClassExpression).distinct()

  // top(M,100)
  def PostProc(): Array[OWLClassExpression] = Action().take(100)

  def Voidify(): RDD[String] = {
    val cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + PostProc() + ";"
    spark.sparkContext.parallelize(cd)
  }


}
object UsedClasses {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): UsedClasses = new UsedClasses(axioms, spark)

}

// Criterion 2.
class UsedClassesCount (axioms: RDD[OWLAxiom], spark: SparkSession) {

  // ?p=rdf:type && isIRI(?o)
  // ClassAssertions (C, indv)
  def Filter(): RDD[OWLClassExpression] = {

    val usedClasses = owlAxioms.extractAxiom(axioms, AxiomType.CLASS_ASSERTION)
                               .asInstanceOf[RDD[OWLClassAssertionAxiom]]
                               .map(_.getClassExpression)

    usedClasses
  }

  // M[?o]++
  def Action(): RDD[(OWLClassExpression, Int)] = Filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def PostProc(): Array[(OWLClassExpression, Int)] = Action().sortBy(_._2, false).take(100)

  def Voidify(): RDD[String] = {

    var axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:classPartition "

    val classes = spark.sparkContext.parallelize(PostProc())
    val vc = classes.map(t => "[ void:class " + "<" + t._1 + ">;   void:axioms " + t._2 + "; ], ")

    var c_action = new Array[String](1)
    c_action(0) = "\nvoid:classes " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(axiomsString)
    val c = spark.sparkContext.parallelize(c_action)
    if (classes.count() > 0) {
      c.union(c_p).union(vc)
    } else c.union(vc)
  }
}

object UsedClassesCount {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): UsedClassesCount = new UsedClassesCount(axioms, spark)

}

// Criterion 3.
class DefinedClasses (axioms: RDD[OWLAxiom], spark: SparkSession) {

  // ?p=rdf:type && isIRI(?s) &&(?o=rdfs:Class||?o=owl:Class)
  // isIRI(C) && Declaration(Class(C))
  def Filter(): RDD[OWLDeclarationAxiom] = {

    val declaration = owlAxioms.extractAxiom(axioms, AxiomType.DECLARATION).asInstanceOf[RDD[OWLDeclarationAxiom]]

    val classesDeclarations = declaration.filter(a => a.getEntity.isOWLClass)

    classesDeclarations
  }

  def Action(): RDD[IRI] = Filter().map(_.getEntity.getIRI)

  def PostProc(): Long = Action().count()

  def Voidify(): RDD[String] = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + PostProc() + ";"
    spark.sparkContext.parallelize(cd)
  }
}

object DefinedClasses {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): DefinedClasses = new DefinedClasses(axioms, spark)
}

// Criterion 5(1).
class UsedDataProperties (axioms: RDD[OWLAxiom], spark: SparkSession) {

  def Filter(): RDD[OWLDataPropertyExpression] = {

    val dataPropertyAssertion = owlAxioms.extractAxiom(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                         .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val usedDataProperties = dataPropertyAssertion.map(_.getProperty).distinct()

    usedDataProperties
  }

  // M[?p]++
  def Action(): RDD[(OWLDataPropertyExpression, Int)] = Filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def PostProc(): Array[(OWLDataPropertyExpression, Int)] = Action().sortBy(_._2, false).take(100)

  def Voidify(): RDD[String] = {

    var axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:propertyPartition "

    val dataProperties = spark.sparkContext.parallelize(PostProc())
    val vdp = dataProperties.map(t => "[ void:dataProperty " + "<" + t._1 + ">;   void:axioms " + t._2 + "; ], ")

    var dp_action = new Array[String](1)
    dp_action(0) = "\nvoid:dataProperties " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(axiomsString)
    val c = spark.sparkContext.parallelize(dp_action)
    c.union(c_p).union(vdp)
  }
}
object UsedDataProperties {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): UsedDataProperties = new UsedDataProperties(axioms, spark)

}

// Criterion 5(b).
class UsedObjectProperties (axioms: RDD[OWLAxiom], spark: SparkSession) {

  def Filter(): RDD[OWLObjectPropertyExpression] = {

    val objPropertyAssertion = owlAxioms.extractAxiom(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
                                        .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]

    val usedObjectProperties = objPropertyAssertion.map(_.getProperty)

    usedObjectProperties
  }

  // M[?p]++
  def Action(): RDD[(OWLObjectPropertyExpression, Int)] = Filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def PostProc(): Array[(OWLObjectPropertyExpression, Int)] = Action().sortBy(_._2, false).take(100)

  def Voidify(): RDD[String] = {

    val axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:propertyPartition "

    val objProperties = spark.sparkContext.parallelize(PostProc())
    val vop = objProperties.map(t => "[ void:objectProperty " + t._1 + ";   void:axioms " + t._2 + "; ], ")

    val op_action = new Array[String](1)
    op_action(0) = "\nvoid:objectProperties " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(axiomsString)
    val c = spark.sparkContext.parallelize(op_action)
    c.union(c_p).union(vop)
  }
}
object UsedObjectProperties {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): UsedObjectProperties = new UsedObjectProperties(axioms, spark)

}

// Criterion 5(c)
class UsedAnnotationProperties (axioms: RDD[OWLAxiom], spark: SparkSession) {

  def Filter(): RDD[OWLAnnotationProperty] = {

    val annAssertion = owlAxioms.extractAxiom(axioms, AxiomType.ANNOTATION_ASSERTION).asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]

    val usedAnnProperties = annAssertion.map(_.getProperty)

    usedAnnProperties
  }

  // M[?p]++
  def Action(): RDD[(OWLAnnotationProperty, Int)] = Filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def PostProc(): Array[(OWLAnnotationProperty, Int)] = Action().sortBy(_._2, false).take(100)

  def Voidify(): RDD[String] = {

    val axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:propertyPartition "

    val annProperties = spark.sparkContext.parallelize(PostProc())
    val vap = annProperties.map(t => "[ void:annotationProperty " +  t._1 + ";   void:axioms " + t._2 + "; ], ")

    val ap_action = new Array[String](1)
    ap_action(0) = "\nvoid:annotationProperty " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(axiomsString)
    val c = spark.sparkContext.parallelize(ap_action)
    c.union(c_p).union(vap)
  }
}
object UsedAnnotationProperties {

  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): UsedAnnotationProperties =
    new UsedAnnotationProperties(axioms, spark)

}

object OWLStats {

  def main(args: Array[String]): Unit = {

    println("================================")
    println("|  Distributed OWL Statistics  |")
    println("================================")

    /**
      * Create a SparkSession, do so by first creating a SparkConf object to configure the application .
      * 'Local' is a special value that runs Spark on one thread on the local machine, without connecting to a cluster.
      * An application name used to identify the application on the cluster manager's UI.
      */

    @transient val spark = SparkSession.builder
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Distributed OWL Statistics")
      .getOrCreate()

    val input: String = getClass.getResource("/ont_functional.owl").getPath

   // Call the functional syntax OWLAxiom builder
    val axioms = FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark, input).distinct()

 //   val stats = new OWLStats(spark).run(axioms)

 //    val sparkConf = new SparkConf().setMaster("spark://172.18.160.17:3077")

    println("\n\n")

    val m = new OWLStats(spark).MaxPerProperty(axioms)
    m.collect().foreach(println(_))

    spark.stop
  }
}

