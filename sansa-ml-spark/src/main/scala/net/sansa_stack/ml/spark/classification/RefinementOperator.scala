package net.sansa_stack.ml.spark.classification

import java.util.ArrayList
import java.util.HashSet
import java.util.Iterator
import java.util.Collection
import java.util.Set
import java.util.stream.Stream
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import scala.util.Random

import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLDataFactory
import org.semanticweb.owlapi.model.OWLDataProperty
import org.semanticweb.owlapi.model.OWLLiteral
import org.semanticweb.owlapi.model.OWLObjectProperty
import org.semanticweb.owlapi.model.OWLDataPropertyExpression
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression
import org.semanticweb.owlapi.model.OWLClass
import org.semanticweb.owlapi.model.OWLIndividual
import org.semanticweb.owlapi.model.OWLNamedIndividual
import org.semanticweb.owlapi.search.EntitySearcher
import net.sansa_stack.ml.spark.classification._
import net.sansa_stack.ml.spark.classification.KB.KB
import org.apache.spark.rdd.RDD

object RefinementOperator {
	val d: Double = 0.5
}
/*
 * Experimental class
 */
class RefinementOperator(var kb: KB) {

  private var Concepts: RDD[OWLClass] = kb.getClasses
	private var Roles: RDD[OWLObjectProperty] = kb.getRoles
	private var Properties: RDD[OWLDataProperty] = kb.getDataProperties
	private var dataFactory: OWLDataFactory = kb.getDataFactory
	
	/*
	 * Function to generate subsumed random concepts
	 */
	def getSubsumedRandomConcept(currentConcept: OWLClassExpression): OWLClassExpression = {

			val generator: Random = new Random()
			var newConcept: OWLClassExpression = null
			do{
				if (generator.nextDouble() < 0.5)
						newConcept = Concepts.takeSample(true, 1)(0)
				else {
						// new concept restriction 
						var newConceptBase: OWLClassExpression = null
						newConceptBase =
								if (generator.nextDouble() < 0.5) 
										getRandomConcept(kb)
								else 
										Concepts.takeSample(true, 1)(0)
								
								if (generator.nextDouble() < 0.5) {
                    val role : OWLObjectProperty = Roles.takeSample(true, 1)(0)

									newConcept =
											if (generator.nextDouble() < 0.5)
												kb.getDataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
											else
												kb.getDataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
								}
								
								else if ((generator.nextDouble() < 0.75)) {

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
										if (!values.isEmpty)
												kb.getDataFactory.getOWLDataHasValue(dataProperty, values.get(generator.nextInt(values.size)))
										else kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
								}
								
								else if ((generator.nextDouble() < 0.9)) {

									val dataProperty: OWLObjectProperty = Roles.takeSample(true, 1)(0)
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
											if (!values.isEmpty)
														kb.getDataFactory.getOWLObjectHasValue(dataProperty, values.get(generator.nextInt(values.size)))
											else kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
								} 
								
								else
									newConcept = kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
								}
							} while (!kb.getReasoner.isSatisfiable(newConcept));
					newConcept.getNNF
  }

  
/**
 * @param k
 * @return
 */
/*
 * Generate Random Concept
 */

def getRandomConcept(k: KB): OWLClassExpression = {
		var newConcept: OWLClassExpression = null
		val generator: Random = new Random()
		do{
		  newConcept = Concepts.takeSample(true, 1)(0)
			if (generator.nextDouble() < 0.20)
					newConcept
			else {
					var newConceptBase: OWLClassExpression = null
					newConceptBase =
						if (generator.nextDouble() < 0.2) 
								getRandomConcept(k)
						else 
								newConcept

				  if (generator.nextDouble() < 0.75) {    // new role restriction
  
				    val role : OWLObjectProperty = Roles.takeSample(true, 1)(0)
						
					  newConcept =
								if (generator.nextDouble() < 0.5)
										kb.getDataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
								else
										kb.getDataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
				  }
			}      
		} while (newConcept == null || kb.getReasoner.getInstances(newConcept, false).entities().count().toInt == 0);
  
		newConcept
  }
}