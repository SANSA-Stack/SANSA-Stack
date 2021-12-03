package net.sansa_stack.ml.spark.classification.decisionTrees

import java.util.ArrayList
import java.util.Collection
import java.util.HashSet
import java.util.Set
import java.util.stream.Collectors

import net.sansa_stack.ml.spark.classification.decisionTrees.KB
import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.search.EntitySearcher
import scala.util.control.Breaks._
import scala.util.Random

object RefinementOperator {
  val d: Double = 0.5
}
/*
 * Experimental class
 */
class RefinementOperator(var kb: KB) {

  private val Concepts: RDD[OWLClass] = kb.getClasses
  private val ObjectProperties: RDD[OWLObjectProperty] = kb.getObjectProperties
  private val Properties: RDD[OWLDataProperty] = kb.getDataProperties
//  private var dataFactory: OWLDataFactory = kb.getDataFactory()

  /*
   * Function to generate subsumed random concepts
   */
  def getSubsumedRandomConcept(currentConcept: OWLClassExpression): OWLClassExpression = {

    val generator: Random = new Random()
    var newConcept: OWLClassExpression = null
    do {
      if (generator.nextDouble() < 0.5) {
        newConcept = Concepts.takeSample(true, 1)(0)
      }
      else {
        // new concept restriction
        var newConceptBase: OWLClassExpression = null
        newConceptBase =
          if (generator.nextDouble() < 0.5) { getRandomConcept(kb) }
          else { Concepts.takeSample(true, 1)(0) }

        if (generator.nextDouble() < 0.5) {
          val role : OWLObjectProperty = ObjectProperties.takeSample(true, 1)(0)

          newConcept =
            if (generator.nextDouble() < 0.5) {
              kb.getDataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
            }
            else {
              kb.getDataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
            }
        }

        else if (generator.nextDouble() < 0.75) {

          if (Properties.count() != 0)
          {
            val dataProperty: OWLDataProperty = Properties.takeSample(true, 1)(0)

            val individuals: Set[OWLNamedIndividual] = dataProperty.individualsInSignature().collect(Collectors.toSet())

            val inds: ArrayList[OWLNamedIndividual] = new ArrayList[OWLNamedIndividual](individuals)
            val dPV: Set[OWLLiteral] = new HashSet[OWLLiteral]()
            for(i <- 0 until inds.size())
            {
              val element = inds.get(i)
              dPV.addAll(EntitySearcher.getDataPropertyValues(element, dataProperty, kb.getOntology).asInstanceOf[Collection[_ <: OWLLiteral]])
            }

            val values: ArrayList[OWLLiteral] = new ArrayList[OWLLiteral](dPV)
            newConcept =
              if (!values.isEmpty) {
                kb.getDataFactory.getOWLDataHasValue(dataProperty, values.get(generator.nextInt(values.size)))
              }
              else {
                kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
              }
          }
          else {
            newConcept = kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
          }
        }

        else if (generator.nextDouble() < 0.9) {

          val dataProperty: OWLObjectProperty = ObjectProperties.takeSample(true, 1)(0)
          val individuals: Set[OWLNamedIndividual] = dataProperty.individualsInSignature().collect(Collectors.toSet())
          val inds: ArrayList[OWLIndividual] = new ArrayList[OWLIndividual](individuals)
          val objValues: Set[OWLIndividual] = new HashSet[OWLIndividual]()

          for(i <- 0 until inds.size())
          {
            val element = inds.get(i)
            objValues.addAll(EntitySearcher.getObjectPropertyValues(element, dataProperty, kb.getOntology).asInstanceOf[Collection[_ <: OWLIndividual]])
          }

          val values: ArrayList[OWLIndividual] = new ArrayList[OWLIndividual](objValues)
          newConcept =
            if (!values.isEmpty) {
              kb.getDataFactory.getOWLObjectHasValue(dataProperty, values.get(generator.nextInt(values.size)))
            }
            else {
              kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
            }
          }
        else {
          newConcept = kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
        }
      }

    } while (!kb.getReasoner.isSatisfiable(newConcept))
    // while (!kb.reasoner.isEntailed(kb.dataFactory.getOWLSubClassOfAxiom(newConcept, currentConcept)))

    newConcept.getNNF
  }


  /**
    * @param k Knowledgebase
    * @return Random Concept
    */
  /*
   * Generate Random Concept
   */

  def getRandomConcept(k: KB): OWLClassExpression = {
    var newConcept: OWLClassExpression = null
    val generator: Random = new Random()

//    breakable {
      do {
//        if (newConcept == kb.getDataFactory.getOWLThing) {
//          getRandomConcept(k)
//        }
//        else {
          newConcept = Concepts.takeSample(true, 1)(0)
          if (generator.nextDouble() < 0.20) {
            newConcept
          }
          else {
            var newConceptBase: OWLClassExpression = null
            newConceptBase =
              if (generator.nextDouble() < 0.2) {
                getRandomConcept(k)
              }
              else {
                newConcept
              }

            if (generator.nextDouble() < 0.75) { // new role restriction

              val role: OWLObjectProperty = ObjectProperties.takeSample(true, 1)(0)

              newConcept =
                if (generator.nextDouble() < 0.5) {
                  kb.getDataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
                }
                else {
                  kb.getDataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
                }
            }
          }
//        } // else

      } while (newConcept == kb.getDataFactory.getOWLThing || kb.getReasoner.getInstances(newConcept, false).entities().count() == 0)
//    } // breakeable

    newConcept
  }
}
