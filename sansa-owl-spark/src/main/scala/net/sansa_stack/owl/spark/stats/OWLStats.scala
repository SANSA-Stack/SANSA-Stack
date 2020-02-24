package net.sansa_stack.owl.spark.stats

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.vocab.XSDVocabulary
import scala.collection.JavaConverters._
import scala.compat.java8.StreamConverters._

import net.sansa_stack.owl.spark.owlAxioms
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder

/**
  * A distributed implementation of OWL statistics.
  *
  * @author Heba Mohamed
  */
class OWLStats(spark: SparkSession) extends Serializable {

  // val spark: SparkSession = SparkSession.builder().getOrCreate()

  def run (axioms: RDD[OWLAxiom]): RDD[String] = {

    val stats = UsedClassesCount(axioms, spark).voidify()
      .union(UsedDataProperties(axioms, spark).voidify())
      .union(UsedObjectProperties(axioms, spark).voidify())
      .union(UsedAnnotationProperties(axioms, spark).voidify())

    println("\n =========== OWL Statistics ===========\n")
    stats.collect().foreach(println(_))

    stats
  }

  // New Criterion
  def getClasses(axioms: RDD[OWLAxiom]): RDD[OWLClass] = {
    axioms.flatMap {
      case a: HasClassesInSignature => a.classesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()
  }

  def getClassesCount(axioms: RDD[OWLAxiom]): Long = {
    getClasses(axioms).count()
  }

  def getDataProperties(axioms: RDD[OWLAxiom]): RDD[OWLDataProperty] = {
    val dataProperties: RDD[OWLDataProperty] = axioms.flatMap {
      case a: HasDataPropertiesInSignature => a.dataPropertiesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    dataProperties
  }

  def getDataPropertiesCount(axioms: RDD[OWLAxiom]): Long = {
    getDataProperties(axioms).count()
  }

  def getObjectProperties(axioms: RDD[OWLAxiom]): RDD[OWLObjectProperty] = {
    val objectProperties: RDD[OWLObjectProperty] = axioms.flatMap {
      case a: HasObjectPropertiesInSignature => a.objectPropertiesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    objectProperties
  }

  def getObjectPropertiesCount(axioms: RDD[OWLAxiom]): Long = {
    getObjectProperties(axioms).count()
  }

  def getClassAssertionCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION).count()

  def getSubClassAxiomCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF).count()

  def getSubDataPropAxiomCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY).count()

  def getSubObjectPropAxiomCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.SUB_OBJECT_PROPERTY).count()

  def getSubAnnPropAxiomCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF).count()

  def getDiffIndividualsAxiomCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.DIFFERENT_INDIVIDUALS).count()

  // Criterion 14.
  def getAxiomCount(axioms: RDD[OWLAxiom]): Long = axioms.count()

  /**
    * Criterion 17. Literals
    *
    * @param axioms RDD of OWLAxioms
    * @return number of OWLAxioms that express assertions with literals.
    */
  def getLiteralAssertionsCount(axioms: RDD[OWLAxiom]): Long = {

    val dataPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val negativeDataPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                             .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

    val l1 = dataPropAssertion.distinct().count()
    val l2 = negativeDataPropAssertion.distinct().count()

    l1 + l2
  }

  /**
    * Criterion 20. Datatypes
    *
    * @param axioms RDD of axioms
    * @return histogram of types used for literals.
    */
  def getDatatypesHistogram(axioms: RDD[OWLAxiom]): RDD[(IRI, Int)] = {

    val dataPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val negativeDataPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                             .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

    val l1 = dataPropAssertion.filter(a => a.getObject.getDatatype.getIRI.length() != 0)
                              .map(a => (a.getObject.getDatatype.getIRI, 1))
                              .reduceByKey(_ + _)

    val l2 = negativeDataPropAssertion.filter(a => a.getObject.getDatatype.getIRI.length() != 0)
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
  def getLanguagesHistogram(axioms: RDD[OWLAxiom]): RDD[(String, Int)] = {

    val dataPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val negativeDataPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                             .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

    val l1 = dataPropAssertion.filter(a => !a.getObject.getLang.isEmpty)
                              .map(a => (a.getObject.getLang, 1))
                              .reduceByKey(_ + _)

    val l2 = negativeDataPropAssertion.filter(a => !a.getObject.getLang.isEmpty)
                                      .map(a => (a.getObject.getLang, 1))
                                      .reduceByKey(_ + _)
    l1.union(l2)
  }

  /**
    * Criterion 22. Average typed string length criterion.
    *
    * @param axioms RDD of OWLAxioms
    * @return the average typed string length used throughout the input RDD of OWL axioms.
    */
  def getAvgTypedStringLength(axioms: RDD[OWLAxiom]): Double = {

    val dataPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val negativeDataPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                             .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

    val t1 = dataPropAssertion
                .filter(_.getObject.getDatatype.getIRI.equals(XSDVocabulary.STRING.getIRI))
                .map(_.getObject.getLiteral.length)

    val t2 = negativeDataPropAssertion
                .filter(_.getObject.getDatatype.getIRI.equals(XSDVocabulary.STRING.getIRI))
                .map(_.getObject.getLiteral.length)

    t1.union(t2).mean()
  }

  /**
    * Criterion 23. Average untyped string length criterion.
    *
    * @param axioms RDD of OWLAxioms
    * @return the average untyped string length used throughout OWL ontology.
    */
  def getAvgUntypedStringLength(axioms: RDD[OWLAxiom]): Double = {
    val dataPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val negativeDataPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                             .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

    val t1 = dataPropAssertion
                  .filter(!_.getObject.getLang.isEmpty)
                  .map(_.getObject.getLiteral.length)

    val t2 = negativeDataPropAssertion
                  .filter(!_.getObject.getLang.isEmpty)
                  .map(_.getObject.getLiteral.length)

    t1.union(t2).mean()
  }

  /**
    * Criterion 24. Typed subject
    *
    * @param axioms RDD of OWLAxioms
    * @return list of string representation of typed OWL objects.
    */
  def getTypedOWLObjects(axioms: RDD[OWLAxiom]): RDD[String] = {
    val declarations = owlAxioms.extractAxioms(axioms, AxiomType.DECLARATION)
                                .asInstanceOf[RDD[OWLDeclarationAxiom]]
                                .map(a => a.getEntity.toString)

    val classAssertions = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
                                  .asInstanceOf[RDD[OWLClassAssertionAxiom]]
                                  .map(a => a.getIndividual.toString)

    val funcDataProps = owlAxioms.extractAxioms(axioms, AxiomType.FUNCTIONAL_DATA_PROPERTY)
                                .asInstanceOf[RDD[OWLFunctionalDataPropertyAxiom]]
                                .map(a => a.getProperty.toString)

    val funcObjProps = owlAxioms.extractAxioms(axioms, AxiomType.FUNCTIONAL_OBJECT_PROPERTY)
                               .asInstanceOf[RDD[OWLFunctionalObjectPropertyAxiom]]
                               .map(a => a.getProperty.toString)

    val inverseFuncObjProps = owlAxioms.extractAxioms(axioms, AxiomType.INVERSE_FUNCTIONAL_OBJECT_PROPERTY)
                                      .asInstanceOf[RDD[OWLInverseFunctionalObjectPropertyAxiom]]
                                      .map(a => a.getProperty.toString)

    val reflexiveFuncObjProps = owlAxioms.extractAxioms(axioms, AxiomType.REFLEXIVE_OBJECT_PROPERTY)
                                        .asInstanceOf[RDD[OWLReflexiveObjectPropertyAxiom]]
                                        .map(a => a.getProperty.toString)

    val irreflexiveFuncObjProps = owlAxioms.extractAxioms(axioms, AxiomType.IRREFLEXIVE_OBJECT_PROPERTY)
                                          .asInstanceOf[RDD[OWLIrreflexiveObjectPropertyAxiom]]
                                          .map(a => a.getProperty.toString)

    val symmetricObjProps = owlAxioms.extractAxioms(axioms, AxiomType.SYMMETRIC_OBJECT_PROPERTY)
                                    .asInstanceOf[RDD[OWLSymmetricObjectPropertyAxiom]]
                                    .map(a => a.getProperty.toString)

    val asymmetricObjProps = owlAxioms.extractAxioms(axioms, AxiomType.ASYMMETRIC_OBJECT_PROPERTY)
                                     .asInstanceOf[RDD[OWLAsymmetricObjectPropertyAxiom]]
                                     .map(a => a.getProperty.toString)

    val transitiveObjProps = owlAxioms.extractAxioms(axioms, AxiomType.TRANSITIVE_OBJECT_PROPERTY)
                                     .asInstanceOf[RDD[OWLTransitiveObjectPropertyAxiom]]
                                     .map(a => a.getProperty.toString)

    val typedOWLObjects = spark.sparkContext
                         .union(declarations, classAssertions, funcDataProps, funcObjProps,
                                inverseFuncObjProps, reflexiveFuncObjProps, irreflexiveFuncObjProps,
                                symmetricObjProps, asymmetricObjProps, transitiveObjProps)
                         .distinct()

    typedOWLObjects
  }

  def getTypedOWLEntities(axioms: RDD[OWLAxiom]): RDD[OWLEntity] = {
    val declarations = owlAxioms.extractAxioms(axioms, AxiomType.DECLARATION)
      .asInstanceOf[RDD[OWLDeclarationAxiom]]
      .map(a => a.getEntity)

    val classAssertion: RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .map(a => a.getIndividual)
      .filter(_.isNamed)
      .map(_.asOWLNamedIndividual)

    val funcDataProps: RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.FUNCTIONAL_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLFunctionalDataPropertyAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(_.asOWLDataProperty)

    val funcObjProps: RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.FUNCTIONAL_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLFunctionalObjectPropertyAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(_.getNamedProperty)

    val inverseFuncObjProps: RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.INVERSE_FUNCTIONAL_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLInverseFunctionalObjectPropertyAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(_.getNamedProperty)

    val reflexiveFuncObjProps: RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.REFLEXIVE_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLReflexiveObjectPropertyAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(_.getNamedProperty)

    val irreflexiveFuncObjProps: RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.IRREFLEXIVE_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLIrreflexiveObjectPropertyAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(_.getNamedProperty)

    val symmetricObjProps: RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.SYMMETRIC_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSymmetricObjectPropertyAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(_.getNamedProperty)

    val asymmetricObjProps: RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.ASYMMETRIC_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLAsymmetricObjectPropertyAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(_.getNamedProperty)

    val transitiveObjProps: RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.TRANSITIVE_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLTransitiveObjectPropertyAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(_.getNamedProperty)

    val typedOWLEntities: RDD[OWLEntity] = spark.sparkContext
      .union(declarations, classAssertion, funcDataProps, funcObjProps,
        inverseFuncObjProps, reflexiveFuncObjProps, irreflexiveFuncObjProps,
        symmetricObjProps, asymmetricObjProps, transitiveObjProps)
      .distinct()

    typedOWLEntities
  }

  /**
    * Criterion 25. Labeled subjects criterion.
    *
    * @param axioms RDD of triples
    * @return RDD of labeled OWL annotation subjects.
    */
  def getLabeledSubjects(axioms: RDD[OWLAxiom]): RDD[OWLAnnotationSubject] = {
    val annPropAssertion = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_ASSERTION)
                                    .asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]

    annPropAssertion
      .filter(a => a.getProperty.isLabel)
      .map(_.getSubject)
  }

  /**
    *  Criterion 26. SameAs axioms
    *
    * @param axioms RDD of OWLAxioms
    * @return The number of OWL same individuals axioms in the input RDD
    */
  def getSameAsAxiomsCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.SAME_INDIVIDUAL).count()

  /**
    * Criterion 27. Links.
    *
    * Computes the frequencies of links between entities of different namespaces.
    * This measure is directed, i.e. a link from `ns1 -> ns2` is different from `ns2 -> ns1`.
    *
    * TODO: Handle cases of anonymous OWL objects and/or explain here which cases are omitted and why
    *
    * @param axioms RDD of OWLAxioms
    * @return list of namespace combinations and their frequencies.
    */
  def getNamespaceLinks(axioms: RDD[OWLAxiom]): RDD[(String, String, Int)] = {
    val declarationAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.DECLARATION).asInstanceOf[RDD[OWLDeclarationAxiom]]
                        .filter(a => a.getEntity.getIRI.getNamespace != a.getEntity.getEntityType.getIRI.getNamespace)
                        .map(a => ((a.getEntity.getIRI.getNamespace, a.getEntity.getEntityType.getIRI.getNamespace), 1))

    // -------- classes --------------
    // sub class axioms of named classes
    val subClassAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF)
      .asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .filter(a => a.getSubClass.isNamed && a.getSuperClass.isNamed)
      .filter(a => a.getSubClass.asOWLClass().getIRI.getNamespace != a.getSuperClass.asOWLClass().getIRI.getNamespace)
      .map(a => ((a.getSubClass.asOWLClass().getIRI.getNamespace, a.getSuperClass.asOWLClass().getIRI.getNamespace), 1))

    // disjoint classes axioms of only named classes
    val disjointClassesAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_CLASSES)
      .asInstanceOf[RDD[OWLDisjointClassesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLClass
      .flatMap(_.asPairwiseAxioms.asScala)
      .filter(a => a.getOperandsAsList.get(1).asOWLClass().getIRI.getNamespace != a.getOperandsAsList.get(0).asOWLClass().getIRI.getNamespace)
      .map(a => ((a.getOperandsAsList.get(1).asOWLClass().getIRI.getNamespace, a.getOperandsAsList.get(0).asOWLClass().getIRI.getNamespace), 1))

    // ------------- Object Properties ---------------------

    // sub-object properties axioms of named object properties
    val subObjPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .filter(a => a.getSubProperty.isNamed && a.getSuperProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .filter(a => a.getSubProperty.getNamedProperty.getIRI.getNamespace != a.getSuperProperty.getNamedProperty.getIRI.getNamespace)
      .map(a => ((a.getSubProperty.getNamedProperty.getIRI.getNamespace, a.getSuperProperty.getNamedProperty.getIRI.getNamespace), 1))

    // disjoint properties axioms of only named object properties
    val disjointObjPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .getNamedProperty
      .flatMap(_.asPairwiseAxioms.asScala)
      .filter(a => a.getOperandsAsList.get(1).getNamedProperty.getIRI.getNamespace != a.getOperandsAsList.get(0).getNamedProperty.getIRI.getNamespace)
      .map(a => ((a.getOperandsAsList.get(1).getNamedProperty.getIRI.getNamespace, a.getOperandsAsList.get(0).getNamedProperty.getIRI.getNamespace), 1))

    // object property domain axioms of named object properties with named classes as domains
    val objPropDomainAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .filter(a => a.getProperty.isNamed && a.getDomain.isNamed)  // needed to be able to safely call .getNamedProperty  and .asOWLClass
      .filter(a => a.getProperty.getNamedProperty.getIRI.getNamespace != a.getDomain.asOWLClass().getIRI.getNamespace)
      .map(a => ((a.getProperty.getNamedProperty.getIRI.getNamespace, a.getDomain.asOWLClass().getIRI.getNamespace), 1))

    // object property range axioms of named object properies with named classes as range
    val objPropRangeAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .filter(a => a.getProperty.isNamed && a.getRange.isNamed)  // needed to be able to safely call .getNamedProperty and .asOWLClass
      .filter(a => a.getProperty.getNamedProperty.getIRI.getNamespace != a.getRange.asOWLClass().getIRI.getNamespace)
      .map(a => ((a.getProperty.getNamedProperty.getIRI.getNamespace, a.getRange.asOWLClass().getIRI.getNamespace), 1))

    // equivalent object property axioms of only named object properties
    val eqObjectPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be ably to safely call .getNamedProperty
      .flatMap(_.asPairwiseAxioms.asScala)
      .filter(a => a.getOperandsAsList.get(1).getNamedProperty.getIRI.getNamespace != a.getOperandsAsList.get(0).getNamedProperty.getIRI.getNamespace)
      .map(a => ((a.getOperandsAsList.get(1).getNamedProperty.getIRI.getNamespace, a.getOperandsAsList.get(0).getNamedProperty.getIRI.getNamespace), 1))

    // inverse object property axioms of named object properties
    val invObjPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.INVERSE_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLInverseObjectPropertiesAxiom]]
      .filter(a => a.getFirstProperty.isNamed && a.getSecondProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .filter(a => a.getFirstProperty.getNamedProperty.getIRI.getNamespace != a.getSecondProperty.getNamedProperty.getIRI.getNamespace)
      .map(a => ((a.getFirstProperty.getNamedProperty.getIRI.getNamespace, a.getSecondProperty.getNamedProperty.getIRI.getNamespace), 1))

    // ------------- Data Properties ---------------------
    val disjointDataPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointDataPropertiesAxiom]]
      .flatMap(_.asPairwiseAxioms.asScala)
      .filter(a => a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace != a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace)
      .map(a => ((a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace, a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace), 1))

    val subDataPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY).asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
                        .filter(a => a.getSubProperty.asOWLDataProperty().getIRI.getNamespace != a.getSuperProperty.asOWLDataProperty().getIRI.getNamespace)
                        .map(a => ((a.getSubProperty.asOWLDataProperty().getIRI.getNamespace, a.getSuperProperty.asOWLDataProperty().getIRI.getNamespace), 1))

    val dataPropDomainAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
      .filter(_.getDomain.isNamed)  // needed to be able to safely call .asOWLClass
      .filter(a => a.getProperty.asOWLDataProperty().getIRI.getNamespace != a.getDomain.asOWLClass().getIRI.getNamespace)
      .map(a => ((a.getProperty.asOWLDataProperty.getIRI.getNamespace, a.getDomain.asOWLClass().getIRI.getNamespace), 1))

    val dataPropRangeAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_RANGE).asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
                        .filter(a => a.getProperty.asOWLDataProperty().getIRI.getNamespace != a.getRange.getDataRangeType.getIRI.getNamespace)
                        .map(a => ((a.getProperty.asOWLDataProperty.getIRI.getNamespace, a.getRange.getDataRangeType.getIRI.getNamespace), 1))

    val eqDataPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .flatMap(_.asPairwiseAxioms.asScala)
      .filter(a => a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace != a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace)
      .map(a => ((a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace, a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace), 1))

    // ---------- Annotation properties-----------
    val subAnnPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF).asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .filter(a => a.getSubProperty.asOWLAnnotationProperty().getIRI.getNamespace != a.getSuperProperty.asOWLAnnotationProperty().getIRI.getNamespace)
      .map(a => ((a.getSubProperty.asOWLAnnotationProperty().getIRI.getNamespace, a.getSuperProperty.asOWLAnnotationProperty().getIRI.getNamespace), 1))

    val annPropDomainAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_DOMAIN).asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .filter(a => a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace != a.getDomain.getNamespace)
      .map(a => ((a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace, a.getDomain.getNamespace), 1))

    val annPropRangeAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_RANGE).asInstanceOf[RDD[OWLAnnotationPropertyRangeAxiom]]
      .filter(a => a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace != a.getRange.getNamespace)
      .map(a => ((a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace, a.getRange.getNamespace), 1))

    // -------- Assertions --------------
    // same individuals axioms with only named individuals
    val sameIndvAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.SAME_INDIVIDUAL)
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .filter(a => a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI.getNamespace != a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI.getNamespace)
      .map(a => ((a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI.getNamespace, a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI.getNamespace), 1))

    // different individuals axioms with only named individuals
    val diffIndvAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.DIFFERENT_INDIVIDUALS)
      .asInstanceOf[RDD[OWLDifferentIndividualsAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .filter(a => a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI.getNamespace != a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI.getNamespace)
      .map(a => ((a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI.getNamespace, a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI.getNamespace), 1))

    // class assertion axioms of named classes
    val classAssrAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .filter(_.getClassExpression.isNamed)  // needed to be able to safely call .asOWLClass
      .filter(a => a.getIndividual.asOWLNamedIndividual().getIRI.getNamespace != a.getClassExpression.asOWLClass().getIRI.getNamespace)
      .map(a => ((a.getIndividual.asOWLNamedIndividual().getIRI.getNamespace, a.getClassExpression.asOWLClass().getIRI.getNamespace), 1))

    // object property assertion axioms with only named OWL individuals involved
    val objAssrAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => a.getSubject.isNamed && a.getObject.isNamed)  // needed to be able to safely call .asOWLNamedIndividual
      .filter(a => a.getSubject.asOWLNamedIndividual().getIRI.getNamespace != a.getObject.asOWLNamedIndividual().getIRI.getNamespace)
      .map(a => ((a.getSubject.asOWLNamedIndividual().getIRI.getNamespace, a.getObject.asOWLNamedIndividual().getIRI.getNamespace), 1))

    // data property assertion axioms of named individuals
    val dataAssr = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(_.getSubject.isNamed)  // to be able to safely call .asOWLNamedIndividual
//      .filter(a => a.getSubject.asOWLNamedIndividual().getIRI.getNamespace != a.getObject.getDatatype.getIRI.getNamespace)  // this check is not needed as the set of individuals and the set of datatypes are disjoint
      .map(a => ((a.getSubject.asOWLNamedIndividual().getIRI.getNamespace, a.getObject.getDatatype.getIRI.getNamespace), 1))

    val links = spark.sparkContext.union(
        declarationAxiomLinks, subClassAxiomLinks, disjointClassesAxiomLinks, disjointObjPropAxiomLinks,
        disjointDataPropAxiomLinks, subObjPropAxiomLinks, subDataPropAxiomLinks, subAnnPropAxiomLinks,
        objPropDomainAxiomLinks, dataPropDomainAxiomLinks, annPropDomainAxiomLinks, objPropRangeAxiomLinks,
        dataPropRangeAxiomLinks, annPropRangeAxiomLinks, eqObjectPropAxiomLinks, eqDataPropAxiomLinks,
        invObjPropAxiomLinks, sameIndvAxiomLinks, diffIndvAxiomLinks, classAssrAxiomLinks, objAssrAxiomLinks, dataAssr)
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
  def getMaxPerNumericDatatypeProperty(axioms: RDD[OWLAxiom]): RDD[(OWLDataPropertyExpression, Int)] = {

    val dataPropAssrNumbers = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
                                .filter(a =>
                                  a.getObject.getDatatype.isInteger ||
                                    a.getObject.getDatatype.isDouble ||
                                    a.getObject.getDatatype.isFloat)
                                .map(a => (a.getProperty, a.getObject.getLiteral.toInt))

    val negDataPropAssrNumbers = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                   .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]
                                   .filter(a =>
                                      a.getObject.getDatatype.isInteger ||
                                        a.getObject.getDatatype.isDouble ||
                                        a.getObject.getDatatype.isFloat)
                                   .map(a => (a.getProperty, a.getObject.getLiteral.toInt))

    val max = dataPropAssrNumbers.union(negDataPropAssrNumbers)
                          .reduceByKey(_ max _)

    max
  }

  /**
    * Criterion 29. Average value per numeric property {int,float,double}
    *
    * @param axioms RDD of OWLAxioms
    * @return properties with their average values
    */
  def getAvgPerNumericDatatypeProperty(axioms: RDD[OWLAxiom]): RDD[(OWLDataPropertyExpression, Double)] = {
    val dataPropAssrLiterals = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                     .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
                                     .filter(a =>
                                       a.getObject.getDatatype.isInteger ||
                                         a.getObject.getDatatype.isDouble ||
                                         a.getObject.getDatatype.isFloat)
                                     .map(a => (a.getProperty, a.getObject))

    val negDataPropAssrLiterals = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
                                   .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]
                                   .filter(a =>
                                     a.getObject.getDatatype.isInteger ||
                                       a.getObject.getDatatype.isDouble ||
                                       a.getObject.getDatatype.isFloat)
                                   .map(a => (a.getProperty, a.getObject))

    val average = dataPropAssrLiterals.union(negDataPropAssrLiterals)
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
  def filter(): RDD[OWLClassAssertionAxiom] = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
                                                       .asInstanceOf[RDD[OWLClassAssertionAxiom]]

  // M[?o]++
  def action(): RDD[OWLClassExpression] = filter().map(_.getClassExpression).distinct()

  // top(M,100)
  def postProc(): Array[OWLClassExpression] = action().take(100)

  def voidify(): RDD[String] = {
    val cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + postProc() + ";"
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
  def filter(): RDD[OWLClassExpression] = {
    val usedClasses = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
                               .asInstanceOf[RDD[OWLClassAssertionAxiom]]
                               .map(_.getClassExpression)

    usedClasses
  }

  // M[?o]++
  def action(): RDD[(OWLClassExpression, Int)] = filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def postProc(): Array[(OWLClassExpression, Int)] = action().sortBy(_._2, false).take(100)

  def voidify(): RDD[String] = {
    val axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:classPartition "

    val classes = spark.sparkContext.parallelize(postProc())
    val vc = classes.map(t => "[ void:class " + "<" + t._1 + ">;   void:axioms " + t._2 + "; ], ")

    val c_action = new Array[String](1)
    c_action(0) = "\nvoid:classes " + action().map(f => f._1).distinct().count + ";"
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
  def filter(): RDD[OWLDeclarationAxiom] = {

    val declaration = owlAxioms.extractAxioms(axioms, AxiomType.DECLARATION).asInstanceOf[RDD[OWLDeclarationAxiom]]

    val classesDeclarations = declaration.filter(a => a.getEntity.isOWLClass)

    classesDeclarations
  }

  def action(): RDD[IRI] = filter().map(_.getEntity.getIRI)

  def postProc(): Long = action().count()

  def voidify(): RDD[String] = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + postProc() + ";"
    spark.sparkContext.parallelize(cd)
  }
}

object DefinedClasses {
  def apply(axioms: RDD[OWLAxiom], spark: SparkSession): DefinedClasses = new DefinedClasses(axioms, spark)
}

// Criterion 5(1).
class UsedDataProperties (axioms: RDD[OWLAxiom], spark: SparkSession) {
  def filter(): RDD[OWLDataPropertyExpression] = {

    val dataPropertyAssertion = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                                         .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

    val usedDataProperties = dataPropertyAssertion.map(_.getProperty).distinct()

    usedDataProperties
  }

  // M[?p]++
  def action(): RDD[(OWLDataPropertyExpression, Int)] = filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def postProc(): Array[(OWLDataPropertyExpression, Int)] = action().sortBy(_._2, false).take(100)

  def voidify(): RDD[String] = {
    val axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:propertyPartition "

    val dataProperties = spark.sparkContext.parallelize(postProc())
    val vdp = dataProperties.map(t => "[ void:dataProperty " + "<" + t._1 + ">;   void:axioms " + t._2 + "; ], ")

    val dp_action = new Array[String](1)
    dp_action(0) = "\nvoid:dataProperties " + action().map(f => f._1).distinct().count + ";"
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

  def filter(): RDD[OWLObjectPropertyExpression] = {
    val objPropertyAssertion = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
                                        .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]

    val usedObjectProperties = objPropertyAssertion.map(_.getProperty)

    usedObjectProperties
  }

  // M[?p]++
  def action(): RDD[(OWLObjectPropertyExpression, Int)] = filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def postProc(): Array[(OWLObjectPropertyExpression, Int)] = action().sortBy(_._2, false).take(100)

  def voidify(): RDD[String] = {
    val axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:propertyPartition "

    val objProperties = spark.sparkContext.parallelize(postProc())
    val vop = objProperties.map(t => "[ void:objectProperty " + t._1 + ";   void:axioms " + t._2 + "; ], ")

    val op_action = new Array[String](1)
    op_action(0) = "\nvoid:objectProperties " + action().map(f => f._1).distinct().count + ";"
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

  def filter(): RDD[OWLAnnotationProperty] = {

    val annAssertion = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_ASSERTION).asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]
    val usedAnnProperties = annAssertion.map(_.getProperty)

    usedAnnProperties
  }

  // M[?p]++
  def action(): RDD[(OWLAnnotationProperty, Int)] = filter().map(e => (e, 1)).reduceByKey(_ + _)

  // top(M,100)
  def postProc(): Array[(OWLAnnotationProperty, Int)] = action().sortBy(_._2, false).take(100)

  def voidify(): RDD[String] = {
    val axiomsString = new Array[String](1)
    axiomsString(0) = "\nvoid:propertyPartition "

    val annProperties = spark.sparkContext.parallelize(postProc())
    val vap = annProperties.map(t => "[ void:annotationProperty " +  t._1 + ";   void:axioms " + t._2 + "; ], ")

    val ap_action = new Array[String](1)
    ap_action(0) = "\nvoid:annotationProperty " + action().map(f => f._1).distinct().count + ";"
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

    val m = new OWLStats(spark).getMaxPerNumericDatatypeProperty(axioms)
    m.collect().foreach(println(_))

    spark.stop
  }
}
