package net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner

import java.util
import java.util.Collections

import net.sansa_stack.inference.spark.forwardchaining.axioms.TransitiveReasoner
import net.sansa_stack.ml.spark.classification.decisionTrees.KB
import net.sansa_stack.owl.spark.owlAxioms
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model._

import scala.jdk.CollectionConverters.asScalaIteratorConverter

class EntitySearcher (kb: KB) extends Serializable {
  
  val axioms: OWLAxiomsRDD = kb.getAxioms.cache()
  
//  val objAssertion: RDD[OWLAxiom] = kb.getAxioms.filter(a => a.getAxiomType.equals(AxiomType.OBJECT_PROPERTY_ASSERTION))
  
  
  //  val sc: SparkContext = kb.getSparkSession.sparkContext
//  val classAssertionMap: Map[OWLIndividual, OWLClassExpression] = kb.getClassAssertions.map(a => (a.getIndividual, a.getClassExpression)).collect().toMap
//  val classAssertionBC: Broadcast[Map[OWLIndividual, OWLClassExpression]] = sc.broadcast(classAssertionMap)
  
  def containsAxiom(ax: OWLAxiom): Boolean = {
      kb.getClassAssertions.collect().toSet.exists(a => a.equals(ax))
  }
  
  def getClassAssertionAxioms (c: OWLClassExpression): RDD[OWLClassAssertionAxiom] = {
    
    kb.getClassAssertions.filter(a => a.getClassExpression.equals(c))
  }
  
  def getObjectPropertyAssertionAxioms(i: OWLIndividual): Set[OWLObjectPropertyAssertionAxiom] = {
    
    kb.getObjectPropertyAssertions.collect().toSet.filter {a => a.getSubject == i}
  }
  
  def getObjectPropertyAssertionAxioms(p: OWLObjectPropertyExpression): RDD[OWLObjectPropertyAssertionAxiom] = {
    
    kb.getObjectPropertyAssertions.filter {a => a.getProperty == p}
  }
  
  def getSameAsIndividuals (i: OWLNamedIndividual): util.Set[OWLNamedIndividual] = {
    
    val sameIndividuals = kb.getSameIndividuals.filter(a => a.getIndividuals.contains(i))
    val namedIndividuals = sameIndividuals.map(ax => ax.getIndividuals)
                                          .asInstanceOf[util.Set[OWLNamedIndividual]]
    
    namedIndividuals
  }
  
  def getIndividualAxioms(i: OWLIndividual): RDD[OWLIndividualAxiom] = {
    
    val individualAxioms = kb.getAxioms.filter { a =>
      a.getAxiomType match {
        case AxiomType.CLASS_ASSERTION =>
          val ca = a.asInstanceOf[OWLClassAssertionAxiom].getIndividual
          ca == i
        
        case AxiomType.SAME_INDIVIDUAL =>
          val ca = a.asInstanceOf[OWLSameIndividualAxiom].getIndividuals
          ca.contains(i)
        
        case AxiomType.DIFFERENT_INDIVIDUALS =>
          val ca = a.asInstanceOf[OWLDifferentIndividualsAxiom].getIndividuals
          ca.contains(i)
        
        case AxiomType.OBJECT_PROPERTY_ASSERTION =>
          val ca = a.asInstanceOf[OWLObjectPropertyAssertionAxiom].getSubject
          ca == i
        
        case AxiomType.DATA_PROPERTY_ASSERTION =>
          val ca = a.asInstanceOf[OWLDataPropertyAssertionAxiom].getSubject
          ca == i
        
        case AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION =>
          val ca = a.asInstanceOf[OWLNegativeDataPropertyAssertionAxiom].getSubject
          ca == i
        
        case AxiomType.NEGATIVE_OBJECT_PROPERTY_ASSERTION =>
          val ca = a.asInstanceOf[OWLNegativeObjectPropertyAssertionAxiom].getSubject
          ca == i
      }
    }.asInstanceOf[RDD[OWLIndividualAxiom]]
    
    individualAxioms
  }
  
//  /** Function to return all direct child expressions of a certain OWLClassExpression.
//   * @param c   OWL class expression
//   * @return    RDD of direct children
//   */

//  def getChildren(c: OWLClassExpression): RDD[OWLClassExpression] = {

//    val subClassOf = kb.getSubClassesAxioms

//    val children = subClassOf.asInstanceOf[RDD[OWLSubClassOfAxiom]]
//                             .filter(a => c == a.getSuperClass)
//                             .map(_.getSubClass).distinct()

//    children
//  }
  
  /** Function to return all direct child expressions of a certain OWLClassExpression.
   * @param c   OWL class expression
   * @return    Set of direct children
   */
  def getChildren(c: OWLClassExpression): util.Set[_ <:OWLClassExpression] = {
    
    val children = c match {
      case concept: OWLObjectIntersectionOf => concept.getOperands
      case concept: OWLObjectUnionOf => concept.getOperands
      case concept: OWLObjectComplementOf => Collections.singleton(concept.getOperand)
      case concept: OWLObjectSomeValuesFrom => Collections.singleton(concept.getFiller)
      case concept: OWLObjectAllValuesFrom => Collections.singleton(concept.getFiller)
      case concept: OWLObjectMinCardinality => Collections.singleton(concept.getFiller)
      case concept: OWLObjectExactCardinality => Collections.singleton(concept.getFiller)
      case concept: OWLObjectMaxCardinality => Collections.singleton(concept.getFiller)
      case _: OWLClass => Collections.emptySet
      case _: OWLObjectHasValue => Collections.emptySet
      case _: OWLObjectHasSelf => Collections.emptySet
      case _: OWLObjectOneOf => Collections.emptySet
      case _: OWLDataSomeValuesFrom => Collections.emptySet
      case _: OWLDataAllValuesFrom => Collections.emptySet
      case _: OWLDataHasValue => Collections.emptySet
      case _: OWLDataMinCardinality => Collections.emptySet
      case _: OWLDataMaxCardinality => Collections.emptySet
      case _: OWLDataExactCardinality => Collections.emptySet
     }
    children
    
  }
  
  
  
  /** Function to return the list of subclasses of a certain OWL class.
   * @param c   OWL class expression
   * @return    RDD of the c-subclasses
   */
  
  def getSubClasses(c: OWLClassExpression): RDD[OWLClassExpression] = {
  
    val tr = new TransitiveReasoner()
    val subClassOf = owlAxioms.extractAxioms(axioms, AxiomType.SUBCLASS_OF)
//    kb.getSubClassesAxioms

    val subClasses = tr.computeTransitiveClosure(subClassOf, AxiomType.SUBCLASS_OF)
                       .asInstanceOf[RDD[OWLSubClassOfAxiom]]
                       .filter(a => c.equals(a.getSuperClass))
                       .map(_.getSubClass).distinct()
//      .coalesce(1)
    subClasses
  }
  
  /** Function to return the list of superclasses of a certain OWL class.
   * @param c   OWL class expression
   * @return    RDD of the c-superclasses
   */
  
  def getSuperClasses(c: OWLClassExpression): RDD[OWLClassExpression] = {
    
    val tr = new TransitiveReasoner()
    val subClassOf = kb.getSubClassesAxioms
    
    val superClasses = tr.computeTransitiveClosure(subClassOf, AxiomType.SUBCLASS_OF)
                         .asInstanceOf[RDD[OWLSubClassOfAxiom]]
                         .filter(a => c.equals(a.getSubClass))
                         .map(_.getSuperClass).distinct()
    
    superClasses
  }
  
  /** Function to return the list of subObjectProperties of a certain OWL Object property.
   * @param obj   OWL object property
   * @return    RDD of the OWL objectProperties
   */
  
  def getSubObjectProperties(obj: OWLObjectPropertyExpression): RDD[OWLObjectPropertyExpression] = {
    
    val tr = new TransitiveReasoner()
    val subObject = kb.getSubObjectPropertyAxioms
    
    val superClasses = tr.computeTransitiveClosure(subObject, AxiomType.SUB_OBJECT_PROPERTY)
      .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]
      .filter(a => obj.equals(a.getSubProperty))
      .map(_.getSuperProperty).distinct()
    
    superClasses
  }
  
  /** Function to return data property values for a certain data property.
   * @param i   OWL Individual
   * @param p   OWL data property expression
   * @return    RDD of the related OWL Literals
   */
  
  def getDataPropertyValues(i: OWLIndividual,
                            p: OWLDataPropertyExpression) : RDD[OWLLiteral] = {
  
    val dataPropertyAssertion = kb.getDataPropertyAssertions
      
    val values = dataPropertyAssertion
                    .filter(a => a.getProperty == p && a.getIndividualsInSignature.contains(i))
                    .map(a => a.getObject).distinct()
    
    values
   }
  
  
  /** Function to return object individuals related to a certain object property.
   * @param i   OWL Individual
   * @param p   OWL object property expression
   * @param ax  input RDD[OWLAxioms]
   * @return    RDD of the related OWL Individuals
   */
  
  def getObjectPropertyValues(i: OWLIndividual,
                              p: OWLObjectPropertyExpression) : RDD[OWLIndividual] = {
    
    val objectPropertyAssertion = kb.getObjectPropertyAssertions
    
    val values = objectPropertyAssertion
                  .filter(a => a.getProperty == p && a.getIndividualsInSignature.contains(i))
                  .map(a => a.getObject).distinct()
     values
  }
  
  
  
  def getAxioms(i: OWLIndividual): RDD[OWLIndividualAxiom] = {
    
    val ax = kb.getAxioms
    val individualAxioms = ax.filter { a =>
                  a.getAxiomType match {
                    case AxiomType.CLASS_ASSERTION =>
                      val ca = a.asInstanceOf[OWLClassAssertionAxiom].getIndividual
                      ca == i

                    case AxiomType.SAME_INDIVIDUAL =>
                      val ca = a.asInstanceOf[OWLSameIndividualAxiom].getIndividuals
                      ca.contains(i)

                    case AxiomType.DIFFERENT_INDIVIDUALS =>
                      val ca = a.asInstanceOf[OWLDifferentIndividualsAxiom].getIndividuals
                      ca.contains(i)

                    case AxiomType.OBJECT_PROPERTY_ASSERTION =>
                      val ca = a.asInstanceOf[OWLObjectPropertyAssertionAxiom].getSubject
                      ca == i

                    case AxiomType.DATA_PROPERTY_ASSERTION =>
                      val ca = a.asInstanceOf[OWLDataPropertyAssertionAxiom].getSubject
                      ca == i

                    case AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION =>
                      val ca = a.asInstanceOf[OWLNegativeDataPropertyAssertionAxiom].getSubject
                      ca == i

                    case AxiomType.NEGATIVE_OBJECT_PROPERTY_ASSERTION =>
                      val ca = a.asInstanceOf[OWLNegativeObjectPropertyAssertionAxiom].getSubject
                      ca == i
                  }
                }.asInstanceOf[RDD[OWLIndividualAxiom]]
  
    individualAxioms
  }
  
  def getEquivalentClasses(cls: OWLClassExpression): RDD[OWLClass] = {
    
    val equivalentAxioms = owlAxioms.extractAxioms(axioms, AxiomType.EQUIVALENT_CLASSES)
                                    .asInstanceOf[RDD[OWLEquivalentClassesAxiom]].cache()
    
    val equivClasses = equivalentAxioms.filter { a => a.getOperandsAsList.contains(cls)}
                                       .flatMap {a => a.namedClasses().iterator().asScala}
      
     equivClasses
  }
  
//  def getReferencingAxioms(e: OWLEntity): Set[OWLAxiom] = {
//    asSet(referencingAxioms(e))
//  }
//
//  def referencingAxioms(e: OWLPrimitive): Stream[OWLAxiom] = {
//
//    e match {
//      case entity: OWLEntity =>
//        getReferencingAxioms(entity)
//
//      case iri: IRI =>
//    case _ =>
//    }
//  }
  
}
