package net.sansa_stack.owl.spark.rdd

import org.apache.spark.broadcast.Broadcast
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.util.OWLAPIStreamUtils.asList

 object RefineOWLAxioms extends Serializable {

  private def manager = OWLManager.createOWLOntologyManager()

  private def dataFactory = manager.getOWLDataFactory

  val parallelism = 240

  def refineOWLAxiom (axiom: OWLAxiom,
                       dataPropertiesBC: Broadcast[Array[IRI]],
                       objPropertiesBC: Broadcast[Array[IRI]],
                       annPropertiesBC: Broadcast[Array[IRI]]): OWLAxiom = {

     val T = axiom.getAxiomType

     val correctAxiom: OWLAxiom = T match {
        case AxiomType.ANNOTATION_PROPERTY_DOMAIN =>
          val correctPropertyDomainAxioms = fixWrongPropertyDomainAxioms(axiom, dataPropertiesBC, objPropertiesBC)
          correctPropertyDomainAxioms

        case AxiomType.SUB_ANNOTATION_PROPERTY_OF =>
          val correctSubPropertyAxioms = fixWrongSubPropertyAxioms(axiom, dataPropertiesBC, objPropertiesBC)
          correctSubPropertyAxioms

        case AxiomType.ANNOTATION_ASSERTION =>
          val correctAssertionAxioms = fixWrongPropertyAssertionAxioms(axiom,
                                                                      dataPropertiesBC, objPropertiesBC, annPropertiesBC)
          correctAssertionAxioms

        case _ => axiom
      }

    correctAxiom
  }

  /**
    * Handler for wrong domain axioms
    * OWLDataPropertyDomain and OWLObjectPropertyDomain
    * converted wrongly to OWLAnnotationPropertyDomain
    */
  private def fixWrongPropertyDomainAxioms( propertyDomainAxiom: OWLAxiom,
                                            dataPropertiesBC: Broadcast[Array[IRI]],
                                            objPropertiesBC: Broadcast[Array[IRI]]): OWLAxiom = {

    val annPropDomAxiom = propertyDomainAxiom.asInstanceOf[OWLAnnotationPropertyDomainAxiom]
    val prop = annPropDomAxiom.getProperty.getIRI
    val domain = annPropDomAxiom.getDomain

    // Annotation property domain axiom turned into object property domain after parsing.
    if (objPropertiesBC.value.contains(prop)) {

      // FIXME: Case of non-atomic class expressions not handled
      val domainClass = dataFactory.getOWLClass(domain)
      val objProp = dataFactory.getOWLObjectProperty(prop)
      val propertyDomainAxiom =
        dataFactory.getOWLObjectPropertyDomainAxiom(objProp, domainClass, asList(annPropDomAxiom.annotations))

      propertyDomainAxiom.asInstanceOf[OWLAxiom]

    } else if (dataPropertiesBC.value.contains(prop)) {

      // Annotation property domain axiom turned into data property domain after parsing.
      // FIXME: Case of non-atomic class expressions not handled
      val domainClass = dataFactory.getOWLClass(domain)
      val dataProp = dataFactory.getOWLDataProperty(prop.toString)
      val propertyDomainAxiom =
        dataFactory.getOWLDataPropertyDomainAxiom(dataProp, domainClass, asList(annPropDomAxiom.annotations))

      propertyDomainAxiom.asInstanceOf[OWLAxiom]

    } else {

      annPropDomAxiom
  }
}

  /**
    * Handler for wrong subPropertyOf axioms
    * OWLSubPropertyOf converted wrongly to OWLSubAnnotationPropertyOf
    * instead of OWLSubDataPropertyOf or OWLSubObjectPropertyOf
    */
  private def fixWrongSubPropertyAxioms(SubAnnotationPropertyAxiom: OWLAxiom,
                                        dataPropertiesBC: Broadcast[Array[IRI]],
                                        objPropertiesBC: Broadcast[Array[IRI]]): OWLAxiom = {

    val subAnnPropAxiom = SubAnnotationPropertyAxiom.asInstanceOf[OWLSubAnnotationPropertyOfAxiom]
    val subProperty = subAnnPropAxiom.getSubProperty.getIRI
    val supProperty = subAnnPropAxiom.getSuperProperty.getIRI

    // SubAnnotationPropertyOf axiom turned into SubObjectPropertyOf after parsing.
    if (objPropertiesBC.value.contains(subProperty)) {

      val subObjProperty = dataFactory.getOWLObjectProperty(subProperty)
      val superObjProperty = dataFactory.getOWLObjectProperty(supProperty)
      val subObjPropertyAxiom =
        dataFactory.getOWLSubObjectPropertyOfAxiom(subObjProperty, superObjProperty, asList(subAnnPropAxiom.annotations))

      subObjPropertyAxiom.asInstanceOf[OWLAxiom]

    } else if (dataPropertiesBC.value.contains(subProperty)) {

      // SubAnnotationPropertyOf axiom turned to SubDataPropertyOf after parsing.
      val subDataProperty = dataFactory.getOWLDataProperty(subProperty)
      val superDataProperty = dataFactory.getOWLDataProperty(supProperty)
      val subDataPropertyAxiom =
        dataFactory.getOWLSubDataPropertyOfAxiom(subDataProperty, superDataProperty, asList(subAnnPropAxiom.annotations))

      subDataPropertyAxiom.asInstanceOf[OWLAxiom]

    } else {

      subAnnPropAxiom
    }
  }

  /**
    * Handler for wrong Assertion axioms
    * OWLDataPropertyAssertion and OWLObjectPropertyAssertion
    * converted wrongly to OWLAnnotationPropertyAssertion
    */

  private def fixWrongPropertyAssertionAxioms (annotationAssertionAxiom: OWLAxiom,
                                               dataPropertiesBC: Broadcast[Array[IRI]],
                                               objPropertiesBC: Broadcast[Array[IRI]],
                                               annPropertiesBC: Broadcast[Array[IRI]]): OWLAxiom = {

    val annAssertionAxiom = annotationAssertionAxiom.asInstanceOf[OWLAnnotationAssertionAxiom]
    val annProperty = annAssertionAxiom.getProperty.getIRI

    // ObjectPropertyAssertion axiom turned into AnnotationAssertion after parsing.
    if (objPropertiesBC.value.contains(annProperty)) {
      val annSubject: IRI = annAssertionAxiom.getSubject.asIRI().get()
      val annValue = annAssertionAxiom.getValue.toString

      val objProperty = dataFactory.getOWLObjectProperty(annProperty)
      val individual1 = dataFactory.getOWLNamedIndividual(annSubject)
      val individual2 = dataFactory.getOWLNamedIndividual(annValue)
      val objPropertyAssertionAxiom =
        dataFactory.getOWLObjectPropertyAssertionAxiom(objProperty, individual1, individual2)

      objPropertyAssertionAxiom.asInstanceOf[OWLAxiom]

    } else if (dataPropertiesBC.value.contains(annProperty)) {

      // dataPropertyAssertion axiom turned into AnnotationAssertion after parsing.

      val annSubject: IRI = annAssertionAxiom.getSubject.asIRI().get()
      val annValue = annAssertionAxiom.getValue.asLiteral().get()

      val dataProperty = dataFactory.getOWLDataProperty(annProperty)
      val individual1 = dataFactory.getOWLNamedIndividual(annSubject)
      val dataPropertyAssertionAxiom =
        dataFactory.getOWLDataPropertyAssertionAxiom(dataProperty, individual1, annValue)

      dataPropertyAssertionAxiom.asInstanceOf[OWLAxiom]

    } else if (annPropertiesBC.value.contains(annProperty)) {

      annAssertionAxiom

    } else {
      null
    }
  } // map

 }
