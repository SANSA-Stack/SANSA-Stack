package net.sansa_stack.ml.spark.classification

import java.util.ArrayList
import java.util.HashSet
import java.util.Iterator
import java.util.Collection
//import java.util.Random
import java.util.Set
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

	private var Concepts: RDD[OWLClassExpression] = kb.getClasses

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
									newConcept = Concepts.map(x => generator.nextInt(Concepts.count.asInstanceOf[Int])).asInstanceOf[OWLClassExpression]

											else {

												// new concept restriction 
												var newConceptBase: OWLClassExpression = null
														newConceptBase =
														if (generator.nextDouble() < 0.5) 
															getRandomConcept(kb)
															else {
																val c = Concepts.zipWithIndex().map{case (x,y) => (y,x)}
																val nconcept = c.lookup(generator.nextInt(Concepts.count.asInstanceOf[Int])).asInstanceOf[OWLClassExpression]
																		nconcept
															}

								if (generator.nextDouble() < 0.5) {

									val r = Roles.zipWithIndex().map{case (x,y) => (y,x)}
									val role = r.lookup(generator.nextInt(Roles.count.asInstanceOf[Int])).asInstanceOf[OWLObjectProperty]

											newConcept =
											if (generator.nextDouble() < 0.5)
												kb.getDataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
												else
													kb.getDataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
								}
								else if ((generator.nextDouble() < 0.75)) {


									val o = Properties.zipWithIndex().map{case (x,y) => (y,x)}
									val owlDataProperty: OWLDataProperty = o.lookup(generator.nextInt(Properties.count.asInstanceOf[Int])).asInstanceOf[OWLDataProperty]

											val inds: ArrayList[OWLNamedIndividual] = new ArrayList[OWLNamedIndividual]()
											val dPV: Set[OWLLiteral] = new HashSet[OWLLiteral]()

											for(i <- 0 until inds.size())
											{
												val element = inds.get(i)
														dPV.addAll(EntitySearcher.getDataPropertyValues(element, owlDataProperty, kb.getOntology).asInstanceOf[Collection[_ <: OWLLiteral]])

											}

											val values: ArrayList[OWLLiteral] = new ArrayList[OWLLiteral](dPV)
													newConcept =
													if (!values.isEmpty)
														kb.getDataFactory.getOWLDataHasValue(owlDataProperty, values.get(generator.nextInt(values.size)))
														else kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)

								} 
								else if ((generator.nextDouble() < 0.9)) {

									val R = Roles.zipWithIndex().map{case (x,y) => (y,x)}
									val owlDataProperty: OWLObjectProperty = R.lookup(generator.nextInt(Roles.count.asInstanceOf[Int])).asInstanceOf[OWLObjectProperty]

											val inds: ArrayList[OWLIndividual] = new ArrayList[OWLIndividual]()
											val objValues: Set[OWLIndividual] = new HashSet[OWLIndividual]()

											for(i <- 0 until inds.size())
											{
												val element = inds.get(i)
														objValues.addAll(EntitySearcher.getObjectPropertyValues(element, owlDataProperty, kb.getOntology).asInstanceOf[Collection[_ <: OWLIndividual]])
											}

											val values: ArrayList[OWLIndividual] = new ArrayList[OWLIndividual](objValues)
													newConcept =
													if (!values.isEmpty)
														kb.getDataFactory.getOWLObjectHasValue(owlDataProperty, values.get(generator.nextInt(values.size)))
														else kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
								} else
									newConcept = kb.getDataFactory.getOWLObjectComplementOf(newConceptBase)
											}
							} while (!kb.getReasoner.isSatisfiable(newConcept));
					newConcept.getNNF
}



/**
 * @param prob
 * @return
 */
/*
 * Generate Random Concept
 */
def getRandomConcept(k: KB): OWLClassExpression = {
		var newConcept: OWLClassExpression = null
				val generator: Random = new Random()
				do{

					val n = Concepts.zipWithIndex().map{case (x,y) => (y,x)}
					newConcept = n.lookup(generator.nextInt(Concepts.count.asInstanceOf[Int])).asInstanceOf[OWLClassExpression]

							if (generator.nextDouble() < 0.2)
								newConcept
								else {
									var newConceptBase: OWLClassExpression = null
											newConceptBase =
											if (generator.nextDouble() < 0.2) 
												getRandomConcept(k)
												else 
													newConcept

													if (generator.nextDouble() < 0.75) {    // new role restriction

														val r = Roles.zipWithIndex().map{case (x,y) => (y,x)}
														val role = r.lookup(generator.nextInt(Roles.count.asInstanceOf[Int])).asInstanceOf[OWLObjectProperty]

																newConcept =
																if (generator.nextDouble() < 0.5)
																	kb.getDataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
																	else
																		kb.getDataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
													}
								}      
				} while (newConcept == null || kb.getReasoner.getInstances(newConcept, false).getFlattened().size() == 0);
newConcept
}
}