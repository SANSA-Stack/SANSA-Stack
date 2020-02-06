package net.sansa_stack.owl.spark

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model._

package object extractOWLAxioms {


  def Declarations (axioms: RDD[OWLAxiom]): RDD[OWLDeclarationAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.DECLARATION))
    .asInstanceOf[RDD[OWLDeclarationAxiom]]

  // ----------- Classes ----------------
  def Classes (axioms: RDD[OWLAxiom]): RDD[OWLClass] = {
    axioms.flatMap {
            case a: HasClassesInSignature => a.classesInSignature().iterator().asScala
            case _ => null
          }.filter(_ != null).distinct()
  }

  def SubClasses (axioms: RDD[OWLAxiom]): RDD[OWLSubClassOfAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.SUBCLASS_OF))
     .asInstanceOf[RDD[OWLSubClassOfAxiom]]

  def EquivalentClasses (axioms: RDD[OWLAxiom]): RDD[OWLEquivalentClassesAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.EQUIVALENT_CLASSES))
    .asInstanceOf[RDD[OWLEquivalentClassesAxiom]]

  def DisjointClasses (axioms: RDD[OWLAxiom]): RDD[OWLDisjointClassesAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.DISJOINT_CLASSES))
    .asInstanceOf[RDD[OWLDisjointClassesAxiom]]

   //  ---------- Data Properties --------------

  def DataProperties (axioms: RDD[OWLAxiom]): RDD[OWLDataProperty] = {
    val dataProperties: RDD[OWLDataProperty] = axioms.flatMap {
        case a: HasDataPropertiesInSignature => a.dataPropertiesInSignature().iterator().asScala
        case _ => null
      }.filter(_ != null).distinct()

    dataProperties
  }

  def SubDataProperty (axioms: RDD[OWLAxiom]): RDD[OWLSubDataPropertyOfAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.SUB_DATA_PROPERTY))
   .asInstanceOf[RDD[OWLSubDataPropertyOfAxiom]]

  def FunctionalDataProperty (axioms: RDD[OWLAxiom]): RDD[OWLFunctionalDataPropertyAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.FUNCTIONAL_DATA_PROPERTY))
    .asInstanceOf[RDD[OWLFunctionalDataPropertyAxiom]]

  def EquivalentDataProperty (axioms: RDD[OWLAxiom]): RDD[OWLEquivalentDataPropertiesAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.EQUIVALENT_DATA_PROPERTIES))
    .asInstanceOf[RDD[OWLEquivalentDataPropertiesAxiom]]

  def DataPropertyRange (axioms: RDD[OWLAxiom]): RDD[OWLDataPropertyRangeAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.DATA_PROPERTY_RANGE))
    .asInstanceOf[RDD[OWLDataPropertyRangeAxiom]]

  def DataPropertyDomain (axioms: RDD[OWLAxiom]): RDD[OWLDataPropertyDomainAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.DATA_PROPERTY_DOMAIN))
   .asInstanceOf[RDD[OWLDataPropertyDomainAxiom]]



  // --------- Object Properties ------------

  def ObjectProperties (axioms: RDD[OWLAxiom]): RDD[OWLObjectProperty] = {
    val objectProperties: RDD[OWLObjectProperty] = axioms.flatMap {
      case a: HasObjectPropertiesInSignature => a.objectPropertiesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()

    objectProperties
  }

  def SubObjectProperty (axioms: RDD[OWLAxiom]): RDD[OWLSubObjectPropertyOfAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.SUB_OBJECT_PROPERTY))
    .asInstanceOf[RDD[OWLSubObjectPropertyOfAxiom]]

  def EquivalentObjectProperty (axioms: RDD[OWLAxiom]): RDD[OWLEquivalentObjectPropertiesAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.EQUIVALENT_OBJECT_PROPERTIES))
   .asInstanceOf[RDD[OWLEquivalentObjectPropertiesAxiom]]

  def ObjectPropertyRange (axioms: RDD[OWLAxiom]): RDD[OWLObjectPropertyRangeAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.OBJECT_PROPERTY_RANGE))
   .asInstanceOf[RDD[OWLObjectPropertyRangeAxiom]]

  def ObjectPropertyDomain (axioms: RDD[OWLAxiom]): RDD[OWLObjectPropertyDomainAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.OBJECT_PROPERTY_DOMAIN))
   .asInstanceOf[RDD[OWLObjectPropertyDomainAxiom]]

  def FunctionalObjectProperty (axioms: RDD[OWLAxiom]): RDD[OWLFunctionalObjectPropertyAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.FUNCTIONAL_OBJECT_PROPERTY))
   .asInstanceOf[RDD[OWLFunctionalObjectPropertyAxiom]]

  def InverseFunctionalObjectProperty (axioms: RDD[OWLAxiom]): RDD[OWLInverseFunctionalObjectPropertyAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.INVERSE_FUNCTIONAL_OBJECT_PROPERTY))
   .asInstanceOf[RDD[OWLInverseFunctionalObjectPropertyAxiom]]

  def ReflexiveFunctionalObjectProperty (axioms: RDD[OWLAxiom]): RDD[OWLReflexiveObjectPropertyAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.REFLEXIVE_OBJECT_PROPERTY))
   .asInstanceOf[RDD[OWLReflexiveObjectPropertyAxiom]]

  def IrreflexiveObjectProperty (axioms: RDD[OWLAxiom]): RDD[OWLIrreflexiveObjectPropertyAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.IRREFLEXIVE_OBJECT_PROPERTY))
   .asInstanceOf[RDD[OWLIrreflexiveObjectPropertyAxiom]]

  def SymmetricObjectProperty (axioms: RDD[OWLAxiom]): RDD[OWLSymmetricObjectPropertyAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.SYMMETRIC_OBJECT_PROPERTY))
   .asInstanceOf[RDD[OWLSymmetricObjectPropertyAxiom]]

  def ASymmetricObjectProperty (axioms: RDD[OWLAxiom]): RDD[OWLAsymmetricObjectPropertyAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.ASYMMETRIC_OBJECT_PROPERTY))
   .asInstanceOf[RDD[OWLAsymmetricObjectPropertyAxiom]]

  def TransitiveObjectProperty(axioms: RDD[OWLAxiom]): RDD[OWLTransitiveObjectPropertyAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.TRANSITIVE_OBJECT_PROPERTY))
   .asInstanceOf[RDD[OWLTransitiveObjectPropertyAxiom]]

  // ---------- Annotations ----------------
  def SubAnnotationProperty(axioms: RDD[OWLAxiom]): RDD[OWLSubAnnotationPropertyOfAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.SUB_ANNOTATION_PROPERTY_OF))
   .asInstanceOf[RDD[OWLSubAnnotationPropertyOfAxiom]]

  def AnnotationPropertyDomain(axioms: RDD[OWLAxiom]): RDD[OWLAnnotationPropertyDomainAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.ANNOTATION_PROPERTY_DOMAIN))
     .asInstanceOf[RDD[OWLAnnotationPropertyDomainAxiom]]

  def AnnotationPropertyRange(axioms: RDD[OWLAxiom]): RDD[OWLAnnotationPropertyRangeAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.ANNOTATION_PROPERTY_RANGE))
   .asInstanceOf[RDD[OWLAnnotationPropertyRangeAxiom]]


  // -------- Assertion Axioms --------------

  def SameIndividuals(axioms: RDD[OWLAxiom]): RDD[OWLSameIndividualAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.SAME_INDIVIDUAL))
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]

  def DifferentIndividuals(axioms: RDD[OWLAxiom]): RDD[OWLDifferentIndividualsAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.DIFFERENT_INDIVIDUALS))
      .asInstanceOf[RDD[OWLDifferentIndividualsAxiom]]

  def ClassAssertion(axioms: RDD[OWLAxiom]): RDD[OWLClassAssertionAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.CLASS_ASSERTION))
    .asInstanceOf[RDD[OWLClassAssertionAxiom]]

  def DataPropertyAssertion (axioms: RDD[OWLAxiom]): RDD[OWLDataPropertyAssertionAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.DATA_PROPERTY_ASSERTION))
    .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]

  def ObjectPropertyAssertion (axioms: RDD[OWLAxiom]): RDD[OWLObjectPropertyAssertionAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.OBJECT_PROPERTY_ASSERTION))
   .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]

  def NegativeDataPropertyAssertion (axioms: RDD[OWLAxiom]): RDD[OWLNegativeDataPropertyAssertionAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION))
    .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]

  def AnnotationPropertyAssertion (axioms: RDD[OWLAxiom]): RDD[OWLAnnotationAssertionAxiom] =
    axioms.filter(a => a.getAxiomType.equals(AxiomType.ANNOTATION_ASSERTION))
     .asInstanceOf[RDD[OWLAnnotationAssertionAxiom]]


}
