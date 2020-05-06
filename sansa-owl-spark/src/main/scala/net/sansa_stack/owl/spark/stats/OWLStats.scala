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

  def getDataPropertyAssertionCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION).count()

  def getObjectPropertyAssertionCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION).count()

  def getAnnotationAssertionCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_ASSERTION).count()

  def getSubClassAxiomCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF).count()

  def getSubDataPropAxiomCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY).count()

  def getSubObjectPropAxiomCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.SUB_OBJECT_PROPERTY).count()

  def getSubAnnPropAxiomCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF).count()

  def getDiffIndividualsAxiomCount(axioms: RDD[OWLAxiom]): Long = owlAxioms.extractAxioms(axioms, AxiomType.DIFFERENT_INDIVIDUALS).count()


  /**
    *  Criterion 4. Class hierarchy depth
    *
    *  @param axioms RDD of OWLAxioms
    *  @return RDD of the hierarchy depth of each OWLClass
    */
  def getClassHierarchyDepth(axioms: RDD[OWLAxiom]): RDD[(String, Int)] = {

    val classes: Array[String] = getClasses(axioms).map(_.toString).collect()

    val subClassOf: Array[(String, String)] = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF)
                              .asInstanceOf[RDD[OWLSubClassOfAxiom]]
                              .filter(a => a.getSubClass.isNamed && a.getSuperClass.isNamed)
                              .map(a => (a.getSubClass.toString, a.getSuperClass.toString))
                              .collect()

    var target = classes(0)
    var depth = 0
    val result: Array[(String, Int)] = Array.ofDim[(String, Int)](classes.length)

    for (i <- 0 until classes.size) {
      depth = 0
      target = classes(i)

      for (j <- 0 until subClassOf.size) {
        for (l <- 0 until subClassOf.size) {
          if (subClassOf.apply(l)._1 == target) {
            depth = depth + 1
            target = subClassOf.apply(l)._2
          }
        }
        result.update(i, (classes(i), depth))
      }
    }

    spark.sparkContext.parallelize(result)
  }

  /**
    * Criterion 6. Property usage distinct per subject
    *
    * @param axioms RDD of OWLAxioms
    * @return the usage of properties grouped by subject
    */

  def getPropertyUsageDistinctPerSubject(axioms: RDD[OWLAxiom]): RDD[(Iterable[OWLAxiom], Int)] = {

    // object property assertion axioms with only named OWL properties involved
    val objAssrAxiom = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => a.getSubject.isNamed)
      .groupBy(_.getSubject)
      .map(a => (a._2.filter(f => f.getProperty.isNamed), 1))
      .asInstanceOf[RDD[(Iterable[OWLAxiom], Int)]]

    // negative object property assertion axioms of named object properties
    val negObjectAssrAxiom = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeObjectPropertyAssertionAxiom]]
      .filter(a => a.getSubject.isNamed)
      .groupBy(_.getSubject)
      .map(a => (a._2.filter(f => f.getProperty.isNamed), 1))
      .asInstanceOf[RDD[(Iterable[OWLAxiom], Int)]]

    // data property assertion axioms of named data properties
    val dataAssrAxiom = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(a => a.getSubject.isNamed)
      .groupBy(_.getSubject)
      .map(a => (a._2.filter(f => f.getProperty.isNamed), 1))
      .asInstanceOf[RDD[(Iterable[OWLAxiom], Int)]]

    // negative data property assertion axioms of data properties
    val negDataAssrAxiom = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]
      .filter(a => a.getSubject.isNamed)
      .groupBy(_.getSubject)
      .map(a => (a._2.filter(f => f.getProperty.isNamed), 1))
      .asInstanceOf[RDD[(Iterable[OWLAxiom], Int)]]

    // annotation property assertion axioms of annotation properties
    val annAssrAxiom = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_ASSERTION)
      .asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]
      .filter(a => a.getSubject.isNamed)
      .groupBy(_.getSubject)
      .map(a => (a._2.filter(f => f.getProperty.isNamed), 1))
      .asInstanceOf[RDD[(Iterable[OWLAxiom], Int)]]

    val propDistinctSubject = spark.sparkContext
      .union(objAssrAxiom, negObjectAssrAxiom, dataAssrAxiom, negDataAssrAxiom, annAssrAxiom)
      .reduceByKey(_ + _)

    propDistinctSubject

  }

  /**
    * Criterion 7. Property usage distinct per object
    *
    * @param axioms RDD of OWLAxioms
    * @return the usage of properties grouped by object
    */

  def getPropertyUsageDistinctPerObject(axioms: RDD[OWLAxiom]): RDD[(Iterable[OWLAxiom], Int)] = {

    // object property assertion axioms with only named OWL properties involved
    val objAssrAxiom = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => a.getObject.isNamed)
      .groupBy(_.getObject)
      .map(a => (a._2.filter(f => f.getProperty.isNamed), 1))
      .asInstanceOf[RDD[(Iterable[OWLAxiom], Int)]]

    // negative object property assertion axioms of named object properties
    val negObjectAssrAxiom = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeObjectPropertyAssertionAxiom]]
      .filter(a => a.getObject.isNamed)
      .groupBy(_.getObject)
      .map(a => (a._2.filter(f => f.getProperty.isNamed), 1))
      .asInstanceOf[RDD[(Iterable[OWLAxiom], Int)]]

    // data property assertion axioms of named data properties
    val dataAssrAxiom = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .groupBy(_.getObject)
      .map(a => (a._2.filter(f => f.getProperty.isNamed), 1))
      .asInstanceOf[RDD[(Iterable[OWLAxiom], Int)]]

    // negative data property assertion axioms of data properties
    val negDataAssrAxiom = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]
      .groupBy(_.getObject)
      .map(a => (a._2.filter(f => f.getProperty.isNamed), 1))
      .asInstanceOf[RDD[(Iterable[OWLAxiom], Int)]]

    // annotation property assertion axioms of annotation properties
    val annAssrAxiom = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_ASSERTION)
      .asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]
      .groupBy(_.getValue)
      .map(a => (a._2.filter(f => f.getProperty.isNamed), 1))
      .asInstanceOf[RDD[(Iterable[OWLAxiom], Int)]]

    val propDistinctObject = spark.sparkContext
      .union(objAssrAxiom, negObjectAssrAxiom, dataAssrAxiom, negDataAssrAxiom, annAssrAxiom)
      .reduceByKey(_ + _)

    propDistinctObject

  }

  /**
    * Criterion 10. Outdegree
    *
    * @param axioms RDD of OWLAxioms
    * @return the average of each outdegree edge
    */

  def getOutdegree(axioms: RDD[OWLAxiom]): RDD[(IRI, Double)] = {

    val declarationAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DECLARATION)
      .asInstanceOf[RDD[OWLDeclarationAxiom]]
      .filter(a => a.getEntity.isNamed)
      .map(a => (a.getEntity.getIRI, 1))

    // -------- classes --------------
    // sub class axioms of named classes
    val subClassAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF)
      .asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .filter(a => a.getSubClass.isNamed)
      .map(a => (a.getSubClass.asOWLClass().getIRI, 1))

    // disjoint classes axioms of only named classes
    val disjointClassesAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_CLASSES)
      .asInstanceOf[RDD[OWLDisjointClassesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLClass
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0).asOWLClass().getIRI, 1))

    // ------------- Object Properties ---------------------

    // sub-object properties axioms of named object properties
    val subObjPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .filter(a => a.getSubProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getSubProperty.getNamedProperty.getIRI, 1))

    // disjoint properties axioms of only named object properties
    val disjointObjPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .getNamedProperty
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0).getNamedProperty.getIRI, 1))

    // object property domain axioms of named object properties with named classes as domains
    val objPropDomainAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .filter(a => a.getProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getProperty.getNamedProperty.getIRI, 1))

    // object property range axioms of named object properties with named classes as range
    val objPropRangeAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .filter(a => a.getProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getProperty.getNamedProperty.getIRI, 1))

    // equivalent object property axioms of only named object properties
    val eqObjectPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be ably to safely call .getNamedProperty
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0).getNamedProperty.getIRI, 1))

    // inverse object property axioms of named object properties
    val invObjPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.INVERSE_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLInverseObjectPropertiesAxiom]]
      .filter(a => a.getFirstProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getFirstProperty.getNamedProperty.getIRI, 1))

    // ------------- Data Properties ---------------------
    val disjointDataPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointDataPropertiesAxiom]]
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0).asOWLDataProperty().getIRI, 1))

    val subDataPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .filter(_.getSubProperty.isNamed)
      .map(a => (a.getSubProperty.asOWLDataProperty().getIRI, 1))

    val dataPropDomainAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLDataProperty.getIRI, 1))

    val dataPropRangeAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLDataProperty.getIRI, 1))

    val eqDataPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0).asOWLDataProperty().getIRI, 1))

    // ---------- Annotation properties-----------
    val subAnnPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
      .asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .filter(_.getSubProperty.isNamed)
      .map(a => (a.getSubProperty.asOWLAnnotationProperty().getIRI, 1))

    val annPropDomainAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLAnnotationProperty().getIRI, 1))

    val annPropRangeAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLAnnotationPropertyRangeAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLAnnotationProperty().getIRI, 1))

    // -------- Assertions --------------
    // same individuals axioms with only named individuals
    val sameIndvAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.SAME_INDIVIDUAL)
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI, 1))

    // different individuals axioms with only named individuals
    val diffIndvAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DIFFERENT_INDIVIDUALS)
      .asInstanceOf[RDD[OWLDifferentIndividualsAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI, 1))

    // class assertion axioms of named individuals
    val classAssrAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .filter(_.getIndividual.isNamed)
      .map(a => (a.getIndividual.asOWLNamedIndividual().getIRI, 1))

    // object property assertion axioms with only named OWL individuals involved
    val objAssrAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => a.getSubject.isNamed)
      .map(a => (a.getSubject.asOWLNamedIndividual().getIRI, 1))

    // negative object property assertion axioms of named individuals
    val negObjectAssrAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeObjectPropertyAssertionAxiom]]
      .filter(_.getSubject.isNamed)
      .map(a => (a.getSubject.asOWLNamedIndividual().getIRI, 1))

    // data property assertion axioms of named individuals
    val dataAssrAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(_.getSubject.isNamed)
      .map(a => (a.getSubject.asOWLNamedIndividual().getIRI, 1))

    // negative data property assertion axioms of named individuals
    val negDataAssrAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]
      .filter(_.getSubject.isNamed)
      .map(a => (a.getSubject.asOWLNamedIndividual().getIRI, 1))

    val out = spark.sparkContext.union(
      declarationAxiomSubject, subClassAxiomSubject, disjointClassesAxiomSubject, disjointObjPropAxiomSubject,
      disjointDataPropAxiomSubject, subObjPropAxiomSubject, subDataPropAxiomSubject, subAnnPropAxiomSubject,
      objPropDomainAxiomSubject, dataPropDomainAxiomSubject, annPropDomainAxiomSubject, objPropRangeAxiomSubject,
      dataPropRangeAxiomSubject, annPropRangeAxiomSubject, eqObjectPropAxiomSubject, eqDataPropAxiomSubject,
      invObjPropAxiomSubject, sameIndvAxiomSubject, diffIndvAxiomSubject, classAssrAxiomSubject, objAssrAxiomSubject,
      negObjectAssrAxiomSubject, dataAssrAxiomSubject, negDataAssrAxiomSubject)
      .reduceByKey (_ + _)

    val outDegree = out.mapValues(a => (a, 1))
                       .reduceByKey {
                          case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)
                        }.mapValues {
                        case (sum, count) => sum / count.toDouble
                      }

    outDegree

  }

  /**
    * Criterion 11. Indegree
    *
    * @param axioms RDD of OWLAxioms
    * @return the average of each indegree edge
    */

  def getInDegree(axioms: RDD[OWLAxiom]): RDD[(IRI, Double)] = {

    val declarationAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DECLARATION)
      .asInstanceOf[RDD[OWLDeclarationAxiom]]
      .filter(a => a.getEntity.isNamed)
      .map(a => (a.getEntity.getEntityType.getIRI, 1))

    // -------- classes --------------
    // sub class axioms of named classes
    val subClassAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF)
      .asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .filter(a => a.getSuperClass.isNamed)
      .map(a => (a.getSuperClass.asOWLClass().getIRI, 1))

    // disjoint classes axioms of only named classes
    val disjointClassesAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_CLASSES)
      .asInstanceOf[RDD[OWLDisjointClassesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLClass
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(1).asOWLClass().getIRI, 1))

    // ------------- Object Properties ---------------------

    // sub-object properties axioms of named object properties
    val subObjPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .filter(a => a.getSuperProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getSuperProperty.getNamedProperty.getIRI, 1))

    // disjoint properties axioms of only named object properties
    val disjointObjPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .getNamedProperty
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(1).getNamedProperty.getIRI, 1))

    // object property domain axioms with named classes as domains
    val objPropDomainAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .filter(a => a.getDomain.isNamed)
      .map(a => (a.getDomain.asOWLClass().getIRI, 1))

    // object property range axioms with named classes as range
    val objPropRangeAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .filter(a => a.getRange.isNamed)
      .map(a => (a.getRange.asOWLClass().getIRI, 1))

    // equivalent object property axioms of only named object properties
    val eqObjectPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be ably to safely call .getNamedProperty
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(1).getNamedProperty.getIRI, 1))

    // inverse object property axioms of named object properties
    val invObjPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.INVERSE_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLInverseObjectPropertiesAxiom]]
      .filter(a => a.getSecondProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getSecondProperty.getNamedProperty.getIRI, 1))

    // ------------- Data Properties ---------------------
    val disjointDataPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointDataPropertiesAxiom]]
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(1).asOWLDataProperty().getIRI, 1))

    val subDataPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .filter(_.getSuperProperty.isNamed)
      .map(a => (a.getSuperProperty.asOWLDataProperty().getIRI, 1))

    val dataPropDomainAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
      .filter(_.getDomain.isNamed)
      .map(a => (a.getDomain.asOWLClass().getIRI, 1))

    val dataPropRangeAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .filter(_.getRange.isNamed)
      .map(a => (a.getRange.asOWLDatatype().getIRI, 1))

    val eqDataPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(1).asOWLDataProperty().getIRI, 1))

    // ---------- Annotation properties-----------
    val subAnnPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
      .asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .filter(_.getSuperProperty.isNamed)
      .map(a => (a.getSuperProperty.asOWLAnnotationProperty().getIRI, 1))

    val annPropDomainAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .filter(_.getDomain.isIRI)
      .map(a => (a.getDomain.asIRI().get(), 1))

    val annPropRangeAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLAnnotationPropertyRangeAxiom]]
      .filter(_.getRange.isIRI)
      .map(a => (a.getRange.asIRI().get(), 1))

    // -------- Assertions --------------
    // same individuals axioms with only named individuals
    val sameIndvAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.SAME_INDIVIDUAL)
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI, 1))

    // different individuals axioms with only named individuals
    val diffIndvAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DIFFERENT_INDIVIDUALS)
      .asInstanceOf[RDD[OWLDifferentIndividualsAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI, 1))

    // class assertion axioms of named individuals
    val classAssrAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .filter(_.getClassExpression.isNamed)
      .map(a => (a.getClassExpression.asOWLClass().getIRI, 1))

    // object property assertion axioms with only named OWL individuals involved
    val objAssrAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => a.getObject.isNamed)
      .map(a => (a.getObject.asOWLNamedIndividual().getIRI, 1))

    // negative object property assertion axioms of named individuals
    val negObjectAssrAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeObjectPropertyAssertionAxiom]]
      .filter(_.getObject.isNamed)
      .map(a => (a.getObject.asOWLNamedIndividual().getIRI, 1))

    val in = spark.sparkContext.union(
      declarationAxiomObject, subClassAxiomObject, disjointClassesAxiomObject, disjointObjPropAxiomObject,
      disjointDataPropAxiomObject, subObjPropAxiomObject, subDataPropAxiomObject, subAnnPropAxiomObject,
      objPropDomainAxiomObject, dataPropDomainAxiomObject, annPropDomainAxiomObject, objPropRangeAxiomObject,
      dataPropRangeAxiomObject, annPropRangeAxiomObject, eqObjectPropAxiomObject, eqDataPropAxiomObject,
      invObjPropAxiomObject, sameIndvAxiomObject, diffIndvAxiomObject, classAssrAxiomObject, objAssrAxiomObject,
      negObjectAssrAxiomObject)
      .reduceByKey(_ + _)

    val inDegree = in.mapValues(a => (a, 1))
                     .reduceByKey {
                        case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)
                     }.mapValues {
                     case (sum, count) => sum / count.toDouble
                    }

    inDegree
  }

  /**
    *  Criterion 12a. Data Property hierarchy depth
    *
    *  @param axioms RDD of OWLAxioms
    *  @return RDD of the hierarchy depth of each OWLDataProperty
    */
  def getDataPropertyHierarchyDepth(axioms: RDD[OWLAxiom]): RDD[(String, Int)] = {

    val dataProperties: Array[String] = getDataProperties(axioms).map(_.toString).collect()

    val subDataProp: Array[(String, String)] = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY)
                               .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
                               .filter(a => a.getSubProperty.isNamed && a.getSuperProperty.isNamed)
                               .map(a => (a.getSubProperty.toString, a.getSuperProperty.toString))
                               .collect()

    var target = dataProperties(0)
    var depth = 0
    val result: Array[(String, Int)] = Array.ofDim[(String, Int)](dataProperties.length)

    for (i <- 0 until dataProperties.size) {
      depth = 0
      target = dataProperties(i)

      for (j <- 0 until subDataProp.size) {
        for (l <- 0 until subDataProp.size) {
          if (subDataProp.apply(l)._1 == target) {
            depth = depth + 1
            target = subDataProp.apply(l)._2
          }
        }
        result.update(i, (dataProperties(i), depth))
      }
    }

    spark.sparkContext.parallelize(result)
  }

  /**
    *  Criterion 12b. Object Property hierarchy depth
    *
    *  @param axioms RDD of OWLAxioms
    *  @return RDD of the hierarchy depth of each OWLObjectProperty
    */
  def getObjectPropertyHierarchyDepth(axioms: RDD[OWLAxiom]): RDD[(String, Int)] = {

    val objProperties = getObjectProperties(axioms).map(_.toString).collect()

    val subObjectProp = owlAxioms.extractAxioms(axioms, AxiomType.SUB_OBJECT_PROPERTY)
                               .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
                               .filter(a => a.getSubProperty.isNamed && a.getSuperProperty.isNamed)
                               .map(a => (a.getSubProperty.toString, a.getSuperProperty.toString))
                               .collect()

    var target = objProperties(0)
    var depth = 0
    val result: Array[(String, Int)] = Array.ofDim[(String, Int)](objProperties.length)

    for (i <- 0 until objProperties.size) {
      depth = 0
      target = objProperties(i)

      for (j <- 0 until subObjectProp.size) {
        for (l <- 0 until subObjectProp.size) {
          if (subObjectProp.apply(l)._1 == target) {
            depth = depth + 1
            target = subObjectProp.apply(l)._2
          }
        }
        result.update(i, (objProperties(i), depth))
      }
    }

    spark.sparkContext.parallelize(result)
  }

  /**
    * Criterion 13. Subclass usage
    *
    * @param axioms RDD of OWLAxioms
    * @return the usage of subclasses
    */
  def getSubclassUsage(axioms: RDD[OWLAxiom]): Long = {

    owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF)
      .count()
  }

  // Criterion 14.
  def getAxiomCount(axioms: RDD[OWLAxiom]): Long = axioms.count()

  /**
    * Criterion 15. Mentioned Entities
    *
    * In this criterion we focus on OWLNamed objects and omit anonymous OWL objects.
    *
    * @param axioms RDD of OWLAxioms
    * @return the number of the mentioned entities within the input ontology.
    */

  def getOWLMentionedEntities(axioms: RDD[OWLAxiom]): Long = {

    val declarations = owlAxioms.extractAxioms(axioms, AxiomType.DECLARATION)
      .asInstanceOf[RDD[OWLDeclarationAxiom]]
      .filter(a => a.getEntity.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    // ---------- Classes -------------
    val classAssertion = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .filter(a => a.getIndividual.isNamed && a.getClassExpression.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val subClass = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF)
      .asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .filter(a => a.getSubClass.isNamed && a.getSuperClass.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val equivClasses = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_CLASSES)
      .asInstanceOf[RDD[OWLEquivalentClassesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .asInstanceOf[RDD[OWLAxiom]]

    val disjointClasses = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_CLASSES)
      .asInstanceOf[RDD[OWLDisjointClassesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .asInstanceOf[RDD[OWLAxiom]]

    val disjointUnion1 = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_UNION)
      .asInstanceOf[RDD[OWLDisjointUnionAxiom]]
      .filter(_.getOWLClass.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val disjointUnion2 = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_UNION)
      .asInstanceOf[RDD[OWLDisjointUnionAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .asInstanceOf[RDD[OWLAxiom]]

    val disjointUnion = disjointUnion1.union(disjointUnion2)

    // ------- Data properties -------------
    val subDataProp = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .filter(a => a.getSubProperty.isNamed && a.getSuperProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val dataPropDomain = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
      .filter(a => a.getProperty.isNamed && a.getDomain.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val dataPropRange = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val equivDataProp = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .asInstanceOf[RDD[OWLAxiom]]

    val disjointDataProp = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointDataPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .asInstanceOf[RDD[OWLAxiom]]

    val funcDataProps = owlAxioms.extractAxioms(axioms, AxiomType.FUNCTIONAL_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLFunctionalDataPropertyAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    // ------- Object properties -------------
    val subObjProp = owlAxioms.extractAxioms(axioms, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .filter(a => a.getSubProperty.isNamed && a.getSuperProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val objPropDomain = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .filter(a => a.getProperty.isNamed && a.getDomain.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val objPropRange = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .filter(a => a.getProperty.isNamed && a.getRange.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val equivObjProp = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .asInstanceOf[RDD[OWLAxiom]]

    val disjointObjProp = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .asInstanceOf[RDD[OWLAxiom]]

    val funcObjProps = owlAxioms.extractAxioms(axioms, AxiomType.FUNCTIONAL_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLFunctionalObjectPropertyAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val inverseFuncObjProps = owlAxioms.extractAxioms(axioms, AxiomType.INVERSE_FUNCTIONAL_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLInverseFunctionalObjectPropertyAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val reflexiveFuncObjProps = owlAxioms.extractAxioms(axioms, AxiomType.REFLEXIVE_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLReflexiveObjectPropertyAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val irreflexiveFuncObjProps = owlAxioms.extractAxioms(axioms, AxiomType.IRREFLEXIVE_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLIrreflexiveObjectPropertyAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val symmetricObjProps = owlAxioms.extractAxioms(axioms, AxiomType.SYMMETRIC_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSymmetricObjectPropertyAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val asymmetricObjProps = owlAxioms.extractAxioms(axioms, AxiomType.ASYMMETRIC_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLAsymmetricObjectPropertyAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val transitiveObjProps = owlAxioms.extractAxioms(axioms, AxiomType.TRANSITIVE_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLTransitiveObjectPropertyAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    // ----------- Annotation Properties ------------
    val subAnnProp = owlAxioms.extractAxioms(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
      .asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .filter(a => a.getSubProperty.isNamed && a.getSuperProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val annPropDomain = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val annPropRange = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLAnnotationPropertyRangeAxiom]]
      .filter(_.getProperty.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    // -------- Assertions -------------
    val sameIndvAssr = owlAxioms.extractAxioms(axioms, AxiomType.SAME_INDIVIDUAL)
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))
      .asInstanceOf[RDD[OWLAxiom]]

    // different individuals axioms with only named individuals
    val diffIndvAssr = owlAxioms.extractAxioms(axioms, AxiomType.DIFFERENT_INDIVIDUALS)
      .asInstanceOf[RDD[OWLDifferentIndividualsAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))
      .asInstanceOf[RDD[OWLAxiom]]

    // class assertion axioms of named classes
    val classAssr = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .filter(a => a.getClassExpression.isNamed && a.getIndividual.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    // object property assertion axioms with only named OWL individuals involved
    val objPropAssr = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => a.getSubject.isNamed && a.getObject.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    // data property assertion axioms of named individuals
    val dataPropAssr = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(_.getSubject.isNamed)
      .asInstanceOf[RDD[OWLAxiom]]

    val mentionedOWLEntities = spark.sparkContext
            .union(declarations, classAssertion, subClass, disjointClasses, disjointUnion, equivClasses,
              subDataProp, dataPropDomain, dataPropRange, equivDataProp, disjointDataProp, funcDataProps,
              subObjProp, objPropDomain, objPropRange, equivObjProp, disjointObjProp,
              funcObjProps, inverseFuncObjProps, reflexiveFuncObjProps, irreflexiveFuncObjProps,
              symmetricObjProps, asymmetricObjProps, transitiveObjProps, subAnnProp, annPropDomain,
              annPropRange, sameIndvAssr, diffIndvAssr, classAssr, objPropAssr, dataPropAssr)
      .count()

    mentionedOWLEntities
  }
  /**
    * Criterion 16. Distinct Entities
    *
    * In this criterion we focus on OWLNamed objects and omit anonymous OWL objects.
    *
    * @param axioms RDD of OWLAxioms
    * @return RDD of the distinct entities in the input ontology.
    */
  def getOWLDistinctEntities(axioms: RDD[OWLAxiom]): RDD[OWLEntity] = {

    val declarations = owlAxioms.extractAxioms(axioms, AxiomType.DECLARATION)
      .asInstanceOf[RDD[OWLDeclarationAxiom]]
      .map(a => a.getEntity)

    // ---------- Classes -------------
    val classAssertion : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .map(a => (a.getIndividual, a.getClassExpression))
      .filter(a => a._1.isNamed)
      .flatMap(a => Seq(a._1.asOWLNamedIndividual(), a._2.asOWLClass()))

    val subClass : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF)
      .asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .map(a => (a.getSubClass, a.getSuperClass))
      .filter(a => a._1.isNamed && a._2.isNamed)
      .flatMap(a => Seq(a._1.asOWLClass(), a._2.asOWLClass()))

    val equivClasses : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_CLASSES)
      .asInstanceOf[RDD[OWLEquivalentClassesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))
      .flatMap(a => Seq(a._1.asOWLClass(), a._2.asOWLClass()))

    val disjointClasses : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_CLASSES)
      .asInstanceOf[RDD[OWLDisjointClassesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLClass
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))
      .filter(a => a._1.isNamed && a._2.isNamed)
      .flatMap(a => Seq(a._1.asOWLClass(), a._2.asOWLClass()))

    val disjointUnion1 : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_UNION)
      .asInstanceOf[RDD[OWLDisjointUnionAxiom]]
      .map(a => a.getOWLClass)
      .filter(_.isNamed)
      .map(a => a.asOWLClass())

    val disjointUnion2 : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_UNION)
      .asInstanceOf[RDD[OWLDisjointUnionAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .flatMap(a => a.getOperandsAsList.iterator().asScala.flatMap(a => Seq(a.asOWLClass())))

    val disjointUnion = disjointUnion1.union(disjointUnion2)

    // ------- Data properties -------------
    val subDataProp : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .map(a => (a.getSubProperty, a.getSuperProperty))
      .filter(a => a._1.isNamed && a._2.isNamed)
      .flatMap(a => Seq(a._1.asOWLDataProperty(), a._2.asOWLDataProperty()))

    val dataPropDomain : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
      .map(a => (a.getProperty, a.getDomain))
      .filter(a => a._1.isNamed && a._2.isNamed)
      .flatMap(a => Seq(a._1.asOWLDataProperty(), a._2.asOWLClass()))

    val dataPropRange : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(a => a.asOWLDataProperty())

    val equivDataProp : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))
      .flatMap(a => Seq(a._1.asOWLDataProperty(), a._2.asOWLDataProperty()))

    val disjointDataProp : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointDataPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))
      .flatMap(a => Seq(a._1.asOWLDataProperty(), a._2.asOWLDataProperty()))

    val funcDataProps: RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.FUNCTIONAL_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLFunctionalDataPropertyAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(_.asOWLDataProperty)

    // ------- Object properties -------------
    val subObjProp : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .map(a => (a.getSubProperty, a.getSuperProperty))
      .filter(a => a._1.isNamed && a._2.isNamed)
      .flatMap(a => Seq(a._1.asOWLObjectProperty(), a._2.asOWLObjectProperty()))

    val objPropDomain : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .map(a => (a.getProperty, a.getDomain))
      .filter(a => a._1.isNamed && a._2.isNamed)
      .flatMap(a => Seq(a._1.asOWLObjectProperty(), a._2.asOWLClass()))

    val objPropRange : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .map(a => (a.getProperty, a.getRange))
      .filter(a => a._1.isNamed && a._2.isNamed)
      .flatMap(a => Seq(a._1.asOWLObjectProperty(), a._2.asOWLClass()))

    val equivObjProp : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))
      .flatMap(a => Seq(a._1.asOWLObjectProperty(), a._2.asOWLObjectProperty()))

    val disjointObjProp : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0), a.getOperandsAsList.get(1)))
      .flatMap(a => Seq(a._1.asOWLObjectProperty(), a._2.asOWLObjectProperty()))

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

    // ----------- Annotation Properties ------------
    val subAnnProp : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
      .asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .map(a => (a.getSubProperty, a.getSuperProperty))
      .filter(a => a._1.isNamed && a._2.isNamed)
      .flatMap(a => Seq(a._1.asOWLAnnotationProperty(), a._2.asOWLAnnotationProperty()))

    val annPropDomain : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(a => a.asOWLAnnotationProperty())

    val annPropRange : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLAnnotationPropertyRangeAxiom]]
      .map(a => a.getProperty)
      .filter(_.isNamed)
      .map(a => a.asOWLAnnotationProperty())

    // -------- Assertions -------------
    val sameIndvAssr : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.SAME_INDIVIDUAL)
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .flatMap(a => Seq(a.getIndividualsAsList.get(1).asOWLNamedIndividual(), a.getIndividualsAsList.get(0).asOWLNamedIndividual()))

    // different individuals axioms with only named individuals
    val diffIndvAssr : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.DIFFERENT_INDIVIDUALS)
      .asInstanceOf[RDD[OWLDifferentIndividualsAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .flatMap(a => Seq(a.getIndividualsAsList.get(1).asOWLNamedIndividual(), a.getIndividualsAsList.get(0).asOWLNamedIndividual()))

    // class assertion axioms of named classes
    val classAssr : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .map(a => (a.getClassExpression, a.getIndividual))
      .filter(a => a._1.isNamed && a._2.isNamed)
      .flatMap(a => Seq(a._1.asOWLClass(), a._2.asOWLNamedIndividual()))

    // object property assertion axioms with only named OWL individuals involved
    val objPropAssr : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => a.getSubject.isNamed && a.getObject.isNamed)  // needed to be able to safely call .asOWLNamedIndividual
      .map(a => (a.getSubject, a.getObject))
      .flatMap(a => Seq(a._1.asOWLNamedIndividual(), a._2.asOWLNamedIndividual()))

    // data property assertion axioms of named individuals
    val dataPropAssr : RDD[OWLEntity] = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(_.getSubject.isNamed)  // to be able to safely call .asOWLNamedIndividual
      .map(a => a.getSubject.asOWLNamedIndividual())

    val distinctOWLEntities: RDD[OWLEntity] = spark.sparkContext
      .union(declarations, classAssertion, subClass, disjointClasses, disjointUnion, equivClasses,
        subDataProp, dataPropDomain, dataPropRange, equivDataProp, disjointDataProp, funcDataProps,
        subObjProp, objPropDomain, objPropRange, equivObjProp, disjointObjProp,
        funcObjProps, inverseFuncObjProps, reflexiveFuncObjProps, irreflexiveFuncObjProps,
        symmetricObjProps, asymmetricObjProps, transitiveObjProps, subAnnProp, annPropDomain,
        annPropRange, sameIndvAssr, diffIndvAssr, classAssr, objPropAssr, dataPropAssr)
      .distinct()

    distinctOWLEntities
  }

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

    val l1 = dataPropAssertion.count()
    val l2 = negativeDataPropAssertion.count()

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

    val l1 = dataPropAssertion.filter(!_.getObject.getLang.isEmpty)
                              .map(a => (a.getObject.getLang, 1))
                              .reduceByKey(_ + _)

    val l2 = negativeDataPropAssertion.filter(!_.getObject.getLang.isEmpty)
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
    * @return list of string representation of typed OWL subjects.
    */

  def getTypedSubject(axioms: RDD[OWLAxiom]): RDD[String] = {

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

  def getSameAsAxiomsCount(axioms: RDD[OWLAxiom]): Long =
          owlAxioms.extractAxioms(axioms, AxiomType.SAME_INDIVIDUAL).count()

  /**
    * Criterion 27. Links.
    *
    * Computes the frequencies of links between entities of different namespaces.
    * This measure is directed, i.e. a link from `ns1 -> ns2` is different from `ns2 -> ns1`.
    *
    * In this criterion we focus on OWLNamed objects and omit anonymous OWL objects.
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

    val subDataPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .filter(a => a.getSubProperty.asOWLDataProperty().getIRI.getNamespace != a.getSuperProperty.asOWLDataProperty().getIRI.getNamespace)
      .map(a => ((a.getSubProperty.asOWLDataProperty().getIRI.getNamespace, a.getSuperProperty.asOWLDataProperty().getIRI.getNamespace), 1))

    val dataPropDomainAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
      .filter(_.getDomain.isNamed)  // needed to be able to safely call .asOWLClass
      .filter(a => a.getProperty.asOWLDataProperty().getIRI.getNamespace != a.getDomain.asOWLClass().getIRI.getNamespace)
      .map(a => ((a.getProperty.asOWLDataProperty.getIRI.getNamespace, a.getDomain.asOWLClass().getIRI.getNamespace), 1))

    val dataPropRangeAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .filter(a => a.getProperty.asOWLDataProperty().getIRI.getNamespace != a.getRange.getDataRangeType.getIRI.getNamespace)
      .map(a => ((a.getProperty.asOWLDataProperty.getIRI.getNamespace, a.getRange.getDataRangeType.getIRI.getNamespace), 1))

    val eqDataPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .flatMap(_.asPairwiseAxioms.asScala)
      .filter(a => a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace != a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace)
      .map(a => ((a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace, a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace), 1))

    // ---------- Annotation properties-----------
    val subAnnPropAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
      .asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .filter(a => a.getSubProperty.asOWLAnnotationProperty().getIRI.getNamespace != a.getSuperProperty.asOWLAnnotationProperty().getIRI.getNamespace)
      .map(a => ((a.getSubProperty.asOWLAnnotationProperty().getIRI.getNamespace, a.getSuperProperty.asOWLAnnotationProperty().getIRI.getNamespace), 1))

    val annPropDomainAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .filter(a => a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace != a.getDomain.getNamespace)
      .map(a => ((a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace, a.getDomain.getNamespace), 1))

    val annPropRangeAxiomLinks = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLAnnotationPropertyRangeAxiom]]
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
    * Criterion 28. Maximum value per property {int, float, double} criterion
    *
    * @param axioms RDD of OWLAxioms
    * @return entities with their maximum values
    */

  def getMaxPerNumericDatatypeProperty(axioms: RDD[OWLAxiom]): RDD[(OWLDataPropertyExpression, Double)] = {

    val max = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
                       .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
                       .filter(a => a.getObject.getDatatype.isInteger ||
                                    a.getObject.getDatatype.isDouble ||
                                    a.getObject.getDatatype.isFloat)
                       .map(a => (a.getProperty, a.getObject.getLiteral.toDouble))
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
                                     .filter(a => a.getObject.getDatatype.isInteger ||
                                         a.getObject.getDatatype.isDouble ||
                                         a.getObject.getDatatype.isFloat)
                                     .map(a => (a.getProperty, a.getObject.getLiteral.toDouble))

    val average = dataPropAssrLiterals.mapValues(a => (a, 1))
                                      .reduceByKey {
                                          case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)
                                      }.mapValues {
                                        case (sum, count) => sum / count
                                      }
    average
  }

  /**
    * Criterion 30. Subject Vocabularies.
    *
    * Computes the frequencies of subject vocabularies.
    *
    * @param axioms RDD of OWLAxioms
    * @return list of subject vocabularies and their frequencies.
    */

  def getSubjectVocabularies(axioms: RDD[OWLAxiom]): RDD[(String, Int)] = {

    val declarationAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DECLARATION)
      .asInstanceOf[RDD[OWLDeclarationAxiom]]
      .filter(a => a.getEntity.isNamed)
      .map(a => (a.getEntity.getIRI.getNamespace, 1))

    // -------- classes --------------
    // sub class axioms of named classes
    val subClassAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF)
      .asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .filter(a => a.getSubClass.isNamed)
      .map(a => (a.getSubClass.asOWLClass().getIRI.getNamespace, 1))

    // disjoint classes axioms of only named classes
    val disjointClassesAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_CLASSES)
      .asInstanceOf[RDD[OWLDisjointClassesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLClass
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0).asOWLClass().getIRI.getNamespace, 1))

    // ------------- Object Properties ---------------------

    // sub-object properties axioms of named object properties
    val subObjPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .filter(a => a.getSubProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getSubProperty.getNamedProperty.getIRI.getNamespace, 1))

    // disjoint properties axioms of only named object properties
    val disjointObjPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .getNamedProperty
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0).getNamedProperty.getIRI.getNamespace, 1))

    // object property domain axioms of named object properties with named classes as domains
    val objPropDomainAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .filter(a => a.getProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getProperty.getNamedProperty.getIRI.getNamespace, 1))

    // object property range axioms of named object properties with named classes as range
    val objPropRangeAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .filter(a => a.getProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getProperty.getNamedProperty.getIRI.getNamespace, 1))

    // equivalent object property axioms of only named object properties
    val eqObjectPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be ably to safely call .getNamedProperty
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0).getNamedProperty.getIRI.getNamespace, 1))

    // inverse object property axioms of named object properties
    val invObjPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.INVERSE_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLInverseObjectPropertiesAxiom]]
      .filter(a => a.getFirstProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getFirstProperty.getNamedProperty.getIRI.getNamespace, 1))

    // ------------- Data Properties ---------------------
    val disjointDataPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointDataPropertiesAxiom]]
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace, 1))

    val subDataPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .filter(_.getSubProperty.isNamed)
      .map(a => (a.getSubProperty.asOWLDataProperty().getIRI.getNamespace, 1))

    val dataPropDomainAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLDataProperty.getIRI.getNamespace, 1))

    val dataPropRangeAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLDataProperty.getIRI.getNamespace, 1))

    val eqDataPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(0).asOWLDataProperty().getIRI.getNamespace, 1))

    // ---------- Annotation properties-----------
    val subAnnPropAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
      .asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .filter(_.getSubProperty.isNamed)
      .map(a => (a.getSubProperty.asOWLAnnotationProperty().getIRI.getNamespace, 1))

    val annPropDomainAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace, 1))

    val annPropRangeAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLAnnotationPropertyRangeAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace, 1))

    // -------- Assertions --------------
    // same individuals axioms with only named individuals
    val sameIndvAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.SAME_INDIVIDUAL)
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI.getNamespace, 1))

    // different individuals axioms with only named individuals
    val diffIndvAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DIFFERENT_INDIVIDUALS)
      .asInstanceOf[RDD[OWLDifferentIndividualsAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getIndividualsAsList.get(0).asOWLNamedIndividual().getIRI.getNamespace, 1))

    // class assertion axioms of named individuals
    val classAssrAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .filter(_.getIndividual.isNamed)
      .map(a => (a.getIndividual.asOWLNamedIndividual().getIRI.getNamespace, 1))

    // object property assertion axioms with only named OWL individuals involved
    val objAssrAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => a.getSubject.isNamed)
      .map(a => (a.getSubject.asOWLNamedIndividual().getIRI.getNamespace, 1))

    // negative object property assertion axioms of named individuals
    val negObjectAssrAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeObjectPropertyAssertionAxiom]]
      .filter(_.getSubject.isNamed)
      .map(a => (a.getSubject.asOWLNamedIndividual().getIRI.getNamespace, 1))

    // data property assertion axioms of named individuals
    val dataAssrAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(_.getSubject.isNamed)
      .map(a => (a.getSubject.asOWLNamedIndividual().getIRI.getNamespace, 1))

    // negative data property assertion axioms of named individuals
    val negDataAssrAxiomSubject = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]
      .filter(_.getSubject.isNamed)
      .map(a => (a.getSubject.asOWLNamedIndividual().getIRI.getNamespace, 1))

    val subjectVocabularies = spark.sparkContext.union(
      declarationAxiomSubject, subClassAxiomSubject, disjointClassesAxiomSubject, disjointObjPropAxiomSubject,
      disjointDataPropAxiomSubject, subObjPropAxiomSubject, subDataPropAxiomSubject, subAnnPropAxiomSubject,
      objPropDomainAxiomSubject, dataPropDomainAxiomSubject, annPropDomainAxiomSubject, objPropRangeAxiomSubject,
      dataPropRangeAxiomSubject, annPropRangeAxiomSubject, eqObjectPropAxiomSubject, eqDataPropAxiomSubject,
      invObjPropAxiomSubject, sameIndvAxiomSubject, diffIndvAxiomSubject, classAssrAxiomSubject, objAssrAxiomSubject,
      negObjectAssrAxiomSubject, dataAssrAxiomSubject, negDataAssrAxiomSubject)
      .reduceByKey(_ + _)
      .map(a => (a._1, a._2))

    subjectVocabularies
  }

  /**
    * Criterion 31. Predicate Vocabularies.
    *
    * Computes the frequencies of Predicate vocabularies.
    *
    * @param axioms RDD of OWLAxioms
    * @return list of Predicate vocabularies and their frequencies.
    */

  def getPredicateVocabularies(axioms: RDD[OWLAxiom]): RDD[(String, Int)] = {

    // object property assertion axioms with only named OWL properties involved
    val objAssrAxiomPredicate = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => a.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLObjectProperty().getIRI.getNamespace, 1))

    // negative object property assertion axioms of named object properties
    val negObjectAssrAxiomPredicate = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeObjectPropertyAssertionAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLObjectProperty().getIRI.getNamespace, 1))

    // data property assertion axioms of named data properties
    val dataAssrAxiomPredicate = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLDataProperty().getIRI.getNamespace, 1))

    // negative data property assertion axioms of data properties
    val negDataAssrAxiomPredicate = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLDataProperty().getIRI.getNamespace, 1))

    // annotation property assertion axioms of annotation properties
    val annAssrAxiomPredicate = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_ASSERTION)
      .asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]
      .filter(_.getProperty.isNamed)
      .map(a => (a.getProperty.asOWLAnnotationProperty().getIRI.getNamespace, 1))

    val predicateVocabularies = spark.sparkContext.union(
      objAssrAxiomPredicate, negObjectAssrAxiomPredicate, dataAssrAxiomPredicate,
      negDataAssrAxiomPredicate, annAssrAxiomPredicate)
      .reduceByKey(_ + _)
      .map(a => (a._1, a._2))

    predicateVocabularies
  }

  /**
    * Criterion 32. Object Vocabularies.
    *
    * Computes the frequencies of object vocabularies.
    *
    * @param axioms RDD of OWLAxioms
    * @return list of object vocabularies and their frequencies.
    */

  def getObjectVocabularies(axioms: RDD[OWLAxiom]): RDD[(String, Int)] = {

    val declarationAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DECLARATION)
      .asInstanceOf[RDD[OWLDeclarationAxiom]]
      .filter(a => a.getEntity.isNamed)
      .map(a => (a.getEntity.getEntityType.getIRI.getNamespace, 1))

    // -------- classes --------------
    // sub class axioms of named classes
    val subClassAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF)
      .asInstanceOf[RDD[OWLSubClassOfAxiom]]
      .filter(a => a.getSuperClass.isNamed)
      .map(a => (a.getSuperClass.asOWLClass().getIRI.getNamespace, 1))

    // disjoint classes axioms of only named classes
    val disjointClassesAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_CLASSES)
      .asInstanceOf[RDD[OWLDisjointClassesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLClass
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(1).asOWLClass().getIRI.getNamespace, 1))

    // ------------- Object Properties ---------------------

    // sub-object properties axioms of named object properties
    val subObjPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .filter(a => a.getSuperProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getSuperProperty.getNamedProperty.getIRI.getNamespace, 1))

    // disjoint properties axioms of only named object properties
    val disjointObjPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be able to safely call .getNamedProperty
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(1).getNamedProperty.getIRI.getNamespace, 1))

    // object property domain axioms with named classes as domains
    val objPropDomainAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]
      .filter(a => a.getDomain.isNamed)
      .map(a => (a.getDomain.asOWLClass().getIRI.getNamespace, 1))

    // object property range axioms with named classes as range
    val objPropRangeAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]
      .filter(a => a.getRange.isNamed)
      .map(a => (a.getRange.asOWLClass().getIRI.getNamespace, 1))

    // equivalent object property axioms of only named object properties
    val eqObjectPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))  // needed to be ably to safely call .getNamedProperty
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(1).getNamedProperty.getIRI.getNamespace, 1))

    // inverse object property axioms of named object properties
    val invObjPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.INVERSE_OBJECT_PROPERTIES)
      .asInstanceOf[RDD[OWLInverseObjectPropertiesAxiom]]
      .filter(a => a.getSecondProperty.isNamed)  // needed to be able to safely call .getNamedProperty
      .map(a => (a.getSecondProperty.getNamedProperty.getIRI.getNamespace, 1))

    // ------------- Data Properties ---------------------
    val disjointDataPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DISJOINT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLDisjointDataPropertiesAxiom]]
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace, 1))

    val subDataPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_DATA_PROPERTY)
      .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]
      .filter(_.getSuperProperty.isNamed)
      .map(a => (a.getSuperProperty.asOWLDataProperty().getIRI.getNamespace, 1))

    val dataPropDomainAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]
      .filter(_.getDomain.isNamed)
      .map(a => (a.getDomain.asOWLClass().getIRI.getNamespace, 1))

    val dataPropRangeAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DATA_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]
      .filter(_.getRange.isNamed)
      .map(a => (a.getRange.asOWLDatatype().getIRI.getNamespace, 1))

    val eqDataPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_DATA_PROPERTIES)
      .asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]
      .filter(a => !a.getOperandsAsList.asScala.exists(_.isAnonymous))
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getOperandsAsList.get(1).asOWLDataProperty().getIRI.getNamespace, 1))

    // ---------- Annotation properties-----------
    val subAnnPropAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.SUB_ANNOTATION_PROPERTY_OF)
      .asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]
      .filter(_.getSuperProperty.isNamed)
      .map(a => (a.getSuperProperty.asOWLAnnotationProperty().getIRI.getNamespace, 1))

    val annPropDomainAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_DOMAIN)
      .asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]
      .filter(_.getDomain.isIRI)
      .map(a => (a.getDomain.asIRI().get().getNamespace, 1))

    val annPropRangeAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.ANNOTATION_PROPERTY_RANGE)
      .asInstanceOf[RDD[OWLAnnotationPropertyRangeAxiom]]
      .filter(_.getRange.isIRI)
      .map(a => (a.getRange.asIRI().get().getNamespace, 1))

    // -------- Assertions --------------
    // same individuals axioms with only named individuals
    val sameIndvAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.SAME_INDIVIDUAL)
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI.getNamespace, 1))

    // different individuals axioms with only named individuals
    val diffIndvAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.DIFFERENT_INDIVIDUALS)
      .asInstanceOf[RDD[OWLDifferentIndividualsAxiom]]
      .filter(a => !a.individuals().toScala.exists(_.isAnonymous))  // needed to be able to safely call .asOWLNamedIndividual
      .flatMap(_.asPairwiseAxioms.asScala)
      .map(a => (a.getIndividualsAsList.get(1).asOWLNamedIndividual().getIRI.getNamespace, 1))

    // class assertion axioms of named individuals
    val classAssrAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
      .filter(_.getClassExpression.isNamed)
      .map(a => (a.getClassExpression.asOWLClass().getIRI.getNamespace, 1))

    // object property assertion axioms with only named OWL individuals involved
    val objAssrAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
      .filter(a => a.getObject.isNamed)
      .map(a => (a.getObject.asOWLNamedIndividual().getIRI.getNamespace, 1))

    // negative object property assertion axioms of named individuals
    val negObjectAssrAxiomObject = owlAxioms.extractAxioms(axioms, AxiomType.NEGATIVE_OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeObjectPropertyAssertionAxiom]]
      .filter(_.getObject.isNamed)
      .map(a => (a.getObject.asOWLNamedIndividual().getIRI.getNamespace, 1))

    val objectVocabularies = spark.sparkContext.union(
              declarationAxiomObject, subClassAxiomObject, disjointClassesAxiomObject, disjointObjPropAxiomObject,
              disjointDataPropAxiomObject, subObjPropAxiomObject, subDataPropAxiomObject, subAnnPropAxiomObject,
              objPropDomainAxiomObject, dataPropDomainAxiomObject, annPropDomainAxiomObject, objPropRangeAxiomObject,
              dataPropRangeAxiomObject, annPropRangeAxiomObject, eqObjectPropAxiomObject, eqDataPropAxiomObject,
              invObjPropAxiomObject, sameIndvAxiomObject, diffIndvAxiomObject, classAssrAxiomObject, objAssrAxiomObject,
              negObjectAssrAxiomObject)
            .reduceByKey(_ + _)
            .map(a => (a._1, a._2))

    objectVocabularies
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

// Criterion 5(a).
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

    val stats = new OWLStats(spark).run(axioms)

    spark.stop

  }

}
