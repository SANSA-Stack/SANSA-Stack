package net.sansa_stack.ml.spark.classification

import java.io.File
import java.net.URI
import java.util.ArrayList
import java.util.HashMap
import java.util.List
import java.util.Random
import java.util.Set
import scala.collection.JavaConversions._
import scala.collection.Iterator

import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLDataFactory
import org.semanticweb.owlapi.model.OWLDataProperty
import org.semanticweb.owlapi.model.OWLIndividual
import org.semanticweb.owlapi.model.OWLLiteral
import org.semanticweb.owlapi.model.OWLNamedIndividual
import org.semanticweb.owlapi.model.OWLClass

import org.semanticweb.HermiT.Reasoner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.Map


import org.apache.spark.{SparkConf, SparkContext}


object KB {
	val d: Double = 0.3
			var generator: Random = new Random(2)


			/*
			 * The class to define the Knowledgebase elements
			 */
			class KB(var UrlOwlFile:String) {

	var ontology: OWLOntology = initKB()
			var reasoner: Reasoner = _
			var manager: OWLOntologyManager = _
			var Concepts: RDD[OWLClassExpression] = _
			var Roles: RDD[OWLObjectProperty] = _
			var dataFactory: OWLDataFactory = _
			var Examples: RDD[OWLIndividual] = _

			//  Date property: property, values and domains
			var dataPropertiesValue: RDD[RDD[OWLLiteral]] = _
			var properties: RDD[OWLDataProperty] = _
			var domain: Array[Array[OWLIndividual]] = _
			var classifications: Array[Array[Int]] = _
			var choiceDataP: Random = new Random(1)
			var choiceObjectP: Random = new Random(1)

			val sparkSession = SparkSession.builder
			.master("local[*]")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.appName("OWL reader example (Functional syntax)")
			.getOrCreate()

			val conf = new SparkConf().setAppName("SparkMe Application").setMaster("local[*]")  // local mode
			val sc = new SparkContext(conf)

			class KB (url:String) {

				UrlOwlFile = url
				ontology = initKB()

			}

			def initKB(): OWLOntology = {

					manager = OWLManager.createOWLOntologyManager()
							println("manager:" + manager)
							val fileURI: URI = URI.create("src/main/resources/Classification/ont_functional.owl")
							dataFactory = manager.getOWLDataFactory

							var ontology: OWLOntology = null

							ontology = manager.loadOntologyFromOntologyDocument(new File(fileURI))

							reasoner = new Reasoner(ontology) 

							println("\nClasses\n-------")

							val classList: Set[OWLClass] = ontology.getClassesInSignature

							var c: Int = 0
							for (cls <- classList){
								if (!cls.isOWLNothing && !cls.isAnonymous) {
									Concepts.map(cls => cls) 
									c+=1
									println(c + " - " + cls)
								}
							}
							println("---------------------------- " + c)
							println("\nProperties\n-------")

							val propList: Set[OWLObjectProperty] = ontology.getObjectPropertiesInSignature
							//Roles = RDD[OWLObjectProperty](propList.size)
							var op: Int = 0
							for (prop <- propList) {
								if (!prop.isAnonymous)
									Roles.map(prop => prop)
									op += 1
									println(prop)
							}
							println("---------------------------- " + op)
							println("\nIndividuals\n-----------")

							val indList: Set[OWLNamedIndividual] = ontology.getIndividualsInSignature
							// Examples = Array.ofDim[OWLIndividual](indList.size)

							var i: Int = 0
							for (ind <- 0 until indList.size()) {
								//Examples(i) = ind.asInstanceOf[OWLIndividual]
								Examples.map(ind => ind)
								i += 1
							}
							println("---------------------------- " + i)
							println("\nKB loaded. \n")
							ontology
			}

			def getClassMembershipResult(testConcepts: RDD[OWLClassExpression], negTestConcepts: RDD[OWLClassExpression],
					examples: RDD[OWLIndividual]): RDD[Array[Int]] = {

							println("\nClassifying all examples ------ ")

							classifications = Array.ofDim[Int](testConcepts.count.asInstanceOf[Int], examples.count.asInstanceOf[Int])
							println("Processed concepts (" + testConcepts.count + "): \n")

							var classify = sc.parallelize(classifications)

							for (c <- 0 until testConcepts.count().asInstanceOf[Int]) {
								var p: Int = 0
										var n: Int = 0
										println("[%d] ", c)

										for (e <- 0 until examples.count.asInstanceOf[Int]) {
											var x = classifications(c)(e) = 0
													classify.map{x => x match {
													case a if(reasoner.isEntailed(dataFactory.getOWLClassAssertionAxiom(testConcepts.map(c => c).asInstanceOf[OWLClassExpression], 
															examples.map(e => e).asInstanceOf[OWLIndividual]))) => +1
															p += 1
													case b if (reasoner.isEntailed(dataFactory.getOWLClassAssertionAxiom(negTestConcepts.map(c => c).asInstanceOf[OWLClassExpression],
															examples.map(e => e).asInstanceOf[OWLIndividual]))) => -1
													case _ => -1
													n += 1

													}
											}

										}// e loop 
							println(": %d  %d \n", p, n)
							} //c loop

							classify
			}

			def setClassMembershipResult(classifications: Array[Array[Int]]): Unit = {
					this.classifications = classifications
			}

			def getDataPropertiesValue(): RDD[RDD[OWLLiteral]] = dataPropertiesValue

					def getClassMembershipResult(): Array[Array[Int]] = classifications


					def getRoleMembershipResult(rols: RDD[OWLObjectProperty], examples: RDD[OWLIndividual]): RDD[Array[Array[Int]]] = {

							println("\nVerifyng all individuals' relationship ------ ")


							val x = rols.count.asInstanceOf[Int]
									val y = examples.count.asInstanceOf[Int]

											val Related: Array[Array[Array[Int]]] = Array.ofDim[Int](x, y, y)
											var RelatedRole = sc.parallelize(Related)

											for (i <- 0 until x ; j <- 0 until y; k <- 0 until y) {

												// I verify that the example related to the example j k with respect to the rule

												var x =  Related(i)(j)(k) = 0

														RelatedRole.map{x => x match {
														case a if (reasoner.isEntailed(dataFactory.getOWLObjectPropertyAssertionAxiom(rols.map(i => i).asInstanceOf[OWLObjectProperty], 
																examples.map(j => j).asInstanceOf[OWLIndividual], examples.map(k => k).asInstanceOf[OWLIndividual]))) => 1
														case _ => -1

														}
												}
											} // for loop

									RelatedRole

					}

					def loadFunctionalDataProperties(): Unit = {
							println("Data Properties--------------")
							val propertiesSet: Set[OWLDataProperty] = ontology.getDataPropertiesInSignature
							val iterator: Iterator[OWLDataProperty] = propertiesSet.iterator()
							val lista: List[OWLDataProperty] = new ArrayList[OWLDataProperty]()
							while (iterator.hasNext) {
								val current: OWLDataProperty = iterator.next()
										lista.add(current)
							}

							// delete the non functional properties   
							var prop = Array.ofDim[OWLDataProperty](lista.size)
									properties = sc.parallelize(prop)

									if (lista.isEmpty)
										throw new RuntimeException("There are functional properties")

										lista.toArray(prop)

										val Length = properties.count.asInstanceOf[Int]
												var domain = Array.ofDim[OWLIndividual](Length, Length)
												var dataPropertiesValue = Array.ofDim[OWLLiteral](Length, Length)

												//dataPropertiesValue = RDD[OWLLiteral] (Length, Length)

												for (i <- 0 until Length) {
													domain(i) = Array.ofDim[OWLIndividual](0)
															val Cartesianproduct: Map[OWLIndividual, Set[OWLLiteral]] = getDatatypeAssertions(prop(i))
															val keys: Set[OWLIndividual] = Cartesianproduct.keySet

															// obtaining single individual domain
															domain(i) = keys.toArray(domain(i))
															dataPropertiesValue(i) = Array.ofDim[OWLLiteral](domain(i).length)

															for (j <- 0 until domain(i).length) {
																val values: Set[OWLLiteral] = Cartesianproduct.get(domain(i)(j)).asInstanceOf[Set[OWLLiteral]]
																		var valuesArray: Array[OWLLiteral] = Array.ofDim[OWLLiteral](0)
																		valuesArray = values.toArray(valuesArray)

																		// the length equal to 1 because the value possible for only one element 1
																		dataPropertiesValue(i)(j) = valuesArray(0)
															}
															//Determine the value for a functional property

												}
					}


					def getDatatypeAssertions(dataProperty: OWLDataProperty): Map[OWLIndividual, Set[OWLLiteral]] = {

							val statements: Map[OWLIndividual, Set[OWLLiteral]] = new HashMap[OWLIndividual, Set[OWLLiteral]]()

									for (ex <- Examples) {
										val dataPropertyValues: Set[OWLLiteral] = reasoner.getDataPropertyValues(ex.asInstanceOf[OWLNamedIndividual], dataProperty)
												statements.put(ex, dataPropertyValues)
									}
					statements
					}

					def getRoles(): RDD[OWLObjectProperty] = Roles

							def getClasses(): RDD[OWLClassExpression] = Concepts

							def getIndividuals(): RDD[OWLIndividual] = Examples

							def getDataProperties(): RDD[OWLDataProperty] = properties

							def getDomains(): Array[Array[OWLIndividual]] = domain

							//def getURL(): String = urlOwlFile

							def getRandomProperty(numQueryProperty: Int): Array[Int] = {
									val queryProperty: Array[Int] = Array.ofDim[Int](numQueryProperty)
											var dataTypeProperty: Int = 0
											while (dataTypeProperty < numQueryProperty) 
											{
												val query: Int = choiceDataP.nextInt(properties.count.asInstanceOf[Int])
														if (domain(query).length > 1) 
														{
															// creation of dataProperty used for the test
															queryProperty(dataTypeProperty) = query 
																	dataTypeProperty += 1

														}
											}
									queryProperty
							}


							def getRandomRoles(numRules: Int): Array[Int] = {
									val RulesTest: Array[Int] = Array.ofDim[Int](numRules)
											for (i <- 0 until numRules)
												RulesTest(i) = choiceObjectP.nextInt(numRules)
												RulesTest
							}

							// Randomly 1-in-law a number of rules on the basis of

							def getReasoner(): Reasoner = reasoner

									def updateExamples(individuals: RDD[OWLIndividual]): Unit = {
											Examples = individuals
							}

							def getDataFactory(): OWLDataFactory = dataFactory

									def getOntology(): OWLOntology = ontology

									def getRandomConcept(): OWLClassExpression = {
											// randomly choose one of the concepts present 
											var newConcept: OWLClassExpression = null

													var BinaryCassification = false
													if (!BinaryCassification){     
														do{
															// case A:  ALC and more expressive ontologies

															newConcept = Concepts.map(x => KB.generator.nextInt(Concepts.count.asInstanceOf[Int])).asInstanceOf[OWLClassExpression]
																	if (KB.generator.nextDouble() < 0.7){
																		val newConceptBase: OWLClassExpression = getRandomConcept
																				if (KB.generator.nextDouble() < 0.1){
																					if (KB.generator.nextDouble() < 0){  // new role restriction

																						val role: OWLObjectProperty = Roles.map(x => KB.generator.nextInt(Roles.count.asInstanceOf[Int])).asInstanceOf[OWLObjectProperty]
																								newConcept =
																								if (KB.generator.nextDouble() < 0.5)
																									dataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
																									else dataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
																					}else
																						newConcept = dataFactory.getOWLObjectComplementOf(newConceptBase)
																				}
																	}
														}while (!reasoner.isSatisfiable(newConcept))
															newConcept
													}
													else {  

														// for less expressive ontologies ALE and so on (only complement to atomic concepts)
														do{
															newConcept = Concepts.map(x => KB.generator.nextInt(Concepts.count.asInstanceOf[Int])).asInstanceOf[OWLClassExpression]

																	if (KB.generator.nextDouble() < d) {
																		val newConceptBase: OWLClassExpression = getRandomConcept
																				if (KB.generator.nextDouble() < d)
																					if (KB.generator.nextDouble() < 0.1) {  // new role restriction
																						val role: OWLObjectProperty = Roles.map(x => KB.generator.nextInt(Roles.count.asInstanceOf[Int])).asInstanceOf[OWLObjectProperty]
																								newConcept = 
																								if (KB.generator.nextDouble() < d)
																									dataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
																									else dataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
																					}
																	} else 
																		newConcept = dataFactory.getOWLObjectComplementOf(newConcept)

														} while (!reasoner.isSatisfiable(newConcept))

															newConcept
													}
									}
}
}
