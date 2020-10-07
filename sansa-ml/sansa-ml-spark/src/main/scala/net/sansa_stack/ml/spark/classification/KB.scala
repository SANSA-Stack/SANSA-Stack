package net.sansa_stack.ml.spark.classification

import java.io.File
import java.net.URI
import java.util.{ ArrayList, List, Random }
import java.util.stream.{ Collectors, IntStream, Stream }

import scala.collection.{ Iterator, Map }
import scala.collection.JavaConverters._
import scala.collection.immutable.{ HashMap, Set }

import collection.JavaConverters._
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.HermiT.{ Configuration, Reasoner, ReasonerFactory }
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.reasoner.{ OWLReasoner, OWLReasonerFactory }
import org.semanticweb.owlapi.reasoner.structural.StructuralReasonerFactory
import org.semanticweb.owlapi.util.SimpleIRIMapper


object KB {
  val d: Double = 0.3
  var generator: Random = new Random(2)

  /**
   * The class to define the Knowledgebase elements
   */

  class KB(var UrlOwlFile: String, rdd: OWLAxiomsRDD, sparkSession: SparkSession) {

    var ontology: OWLOntology = initKB()
    var reasoner: OWLReasoner = _
    var hermit: Reasoner = _
    var manager: OWLOntologyManager = _
    var Concepts: RDD[OWLClass] = _
    var Roles: RDD[OWLObjectProperty] = _
    var dataFactory: OWLDataFactory = _
    var Examples: RDD[OWLIndividual] = _

    //  Date property: property, values and domains
    var dataPropertiesValue: RDD[RDD[OWLLiteral]] = _
    var Properties: RDD[OWLDataProperty] = _
    var domain: Array[Array[OWLIndividual]] = _
    var classifications: Array[Array[Int]] = _
    var choiceDataP: Random = new Random(1)
    var choiceObjectP: Random = new Random(1)

    class KB(url: String) {
      UrlOwlFile = url
      ontology = initKB()
    }

    def initKB(): OWLOntology = {

      val owlFile: File = new File(UrlOwlFile)

      manager = OWLManager.createOWLOntologyManager()

      var ontology: OWLOntology = manager.loadOntologyFromOntologyDocument(owlFile)
      println("\nLoading ontology: \n----------------\n" + ontology)

      // obtain the location where the ontology was loaded from
      var iri = manager.getOntologyDocumentIRI(ontology)

      // Add mapping for the local ontology
      manager.getIRIMappers().add(new SimpleIRIMapper(iri, IRI.create(owlFile)))

      // The data factory provides a point for creating OWL API objects such as classes, properties and individuals.
      dataFactory = manager.getOWLDataFactory

      // Reasoner configuration
      var con: Configuration = new Configuration()
      var reasonerFactory: OWLReasonerFactory = new StructuralReasonerFactory()
      reasoner = reasonerFactory.createReasoner(ontology)

      hermit = new Reasoner(con, ontology)

      // --------- Concepts Extraction -----------------

      val Concepts2: RDD[OWLClass] = rdd.flatMap {
        case axiom: HasClassesInSignature => axiom.classesInSignature().iterator().asScala
        case _ => null
      }.filter(_ != null).distinct()

      Concepts = Concepts2
      println("\n\nConcepts\n-------\n")
      Concepts.take(20).foreach(println(_))

      var nconcepts: Int = Concepts.count.toInt
      println("\nNumber of concepts: " + nconcepts)

      // --------- Object Properties Extraction -----------------

      val Roles2: RDD[OWLObjectProperty] = rdd.map {
        case axiom: HasProperty[OWLObjectProperty] => axiom.getProperty
        case _ => null
      }.filter(_ != null).distinct()

      Roles = Roles2
      println("\nObject Properties\n----------")
      Roles.take(10).foreach(println(_))

      var nobjprop: Int = Roles.count.toInt
      println("\nNumber of object properties: " + nobjprop)

      // -------- Data Properties Extraction --------

      val Properties2: RDD[OWLDataProperty] = rdd.flatMap {
        case axiom: HasDataPropertiesInSignature => axiom.dataPropertiesInSignature().iterator().asScala
        case _ => null
      }.filter(_ != null).distinct()

      Properties = Properties2
      println("\nData Properties\n----------")
      Properties.take(10).foreach(println(_))

      var ndataprop: Int = Properties.count.toInt
      println("\nNumber of data properties: " + ndataprop)

      // --------- Individual Extraction ------------

      val Examples2: RDD[OWLNamedIndividual] = rdd.flatMap {
        case axiom: HasIndividualsInSignature => axiom.individualsInSignature().collect(Collectors.toSet()).asScala
        case _ => null
      }.filter(_ != null).distinct()

      Examples = Examples2.asInstanceOf[RDD[OWLIndividual]]
      println("\nIndividuals\n-----------")
      Examples.take(50).foreach(println(_))

      var nEx: Int = Examples.count.toInt
      println("\nNumber of Individuals: " + nEx)

      println("\nKB loaded. \n")
      ontology
    }

    def getClassMembershipResult(testConcepts: Array[OWLClassExpression], negTestConcepts: Array[OWLClassExpression],
                                 examples: RDD[OWLIndividual]): Array[Array[Int]] = {

      println("\nClassifying all examples \n ------------ ")

      var flag: Boolean = false
      classifications = Array.ofDim[Int](testConcepts.size, examples.count.toInt)
      println("Processed concepts (" + testConcepts.size + "): \n")
      val r: Reasoner = getReasoner

      for (c <- 0 until testConcepts.size) {
        var p: Int = 0
        var n: Int = 0
        println("\nTest Concept number " + (c + 1) + ": " + testConcepts(c))

        for (e <- 0 until examples.count.toInt) {

          classifications(c)(e) = 0
          val ind = examples.take(e + 1).apply(e)

          if (r.isEntailed(getDataFactory.getOWLClassAssertionAxiom(testConcepts(c), ind))) {
            classifications(c)(e) = +1
            p = p + 1
          } else {
            if (!flag) {
              if (r.isEntailed(getDataFactory.getOWLClassAssertionAxiom(negTestConcepts(c), ind))) {
                classifications(c)(e) = -1
              }
            } else {
              classifications(c)(e) = -1
            }

            n = n + 1
          }
        }

        println("\n Pos: " + p + "\t Neg: " + n)
      }

      classifications
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
      var RelatedRole = sparkSession.sparkContext.parallelize(Related)

      for (i <- 0 until x; j <- 0 until y; k <- 0 until y) {

        // verify that the example related to the example j k with respect to the rule
        var x = Related(i)(j)(k) = 0

        RelatedRole.map { x =>
          x match {
            case a if (reasoner.isEntailed(dataFactory.getOWLObjectPropertyAssertionAxiom(
              rols.map(i => i).asInstanceOf[OWLObjectProperty],
              examples.map(j => j).asInstanceOf[OWLIndividual], examples.map(k => k).asInstanceOf[OWLIndividual]))) => 1
            case _ => -1

          }
        }
      } // for loop

      RelatedRole
    }

    def getRoles(): RDD[OWLObjectProperty] = Roles

    def getClasses(): RDD[OWLClass] = Concepts

    def getIndividuals(): RDD[OWLIndividual] = Examples

    def getDataProperties(): RDD[OWLDataProperty] = Properties

    def getDomains(): Array[Array[OWLIndividual]] = domain

    def getDataFactory(): OWLDataFactory = dataFactory

    def getOntology(): OWLOntology = ontology

    def getReasoner(): Reasoner = hermit

    // def getURL(): String = urlOwlFile

    def getRandomProperty(numQueryProperty: Int): Array[Int] = {

      val queryProperty: Array[Int] = Array.ofDim[Int](numQueryProperty)
      var dataTypeProperty: Int = 0
      while (dataTypeProperty < numQueryProperty) {
        val query: Int = choiceDataP.nextInt(Properties.count.asInstanceOf[Int])
        if (domain(query).length > 1) {
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

    def updateExamples(individuals: RDD[OWLIndividual]): Unit = {
      Examples = individuals
    }

    def getRandomConcept(): OWLClassExpression = {
      // randomly choose one of the concepts present
      var newConcept: OWLClassExpression = null

      var BinaryCassification = false
      if (!BinaryCassification) {
        do {
          // case A:  ALC and more expressive ontologies
          newConcept = Concepts.takeSample(true, 1)(0)

          if (KB.generator.nextDouble() < 0.7) {
            val newConceptBase: OWLClassExpression = getRandomConcept
            if (KB.generator.nextDouble() < 0.1) {
              if (KB.generator.nextDouble() < 0) { // new role restriction
                val role: OWLObjectProperty = Roles.takeSample(true, 1)(0)

                newConcept =
                  if (KB.generator.nextDouble() < 0.5) {
                    dataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
                  } else dataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
              } else {
                newConcept = dataFactory.getOWLObjectComplementOf(newConceptBase)
              }
            }
          }
        } while (!reasoner.isSatisfiable(newConcept))
        newConcept

      } else {

        // for less expressive ontologies ALE and so on (only complement to atomic concepts)
        do {
          newConcept = Concepts.takeSample(true, 1)(0)

          if (KB.generator.nextDouble() < d) {
            val newConceptBase: OWLClassExpression = getRandomConcept
            if (KB.generator.nextDouble() < d)
              if (KB.generator.nextDouble() < 0.1) { // new role restriction

                val role: OWLObjectProperty = Roles.takeSample(true, 1)(0)

                newConcept =
                  if (KB.generator.nextDouble() < d) {
                    dataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
                  } else dataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
              }
          } else {
            newConcept = dataFactory.getOWLObjectComplementOf(newConcept)
          }

        } while (!reasoner.isSatisfiable(newConcept))

        newConcept
      }
    }
    /* def loadFunctionalDataProperties(): Unit = {
      println("Data Properties--------------")
      val propertiesSet: Stream[OWLDataProperty] = ontology.dataPropertiesInSignature()
      val iterator: Iterator[OWLDataProperty] = propertiesSet.iterator()
      val lista: List[OWLDataProperty] = new ArrayList[OWLDataProperty]()
      while (iterator.hasNext) {
        val current: OWLDataProperty = iterator.next()
        lista.add(current)
      }

      // delete the non functional properties
      var prop = Array.ofDim[OWLDataProperty](lista.size)
      Properties = sparkSession.sparkContext.parallelize(prop)

      if (lista.isEmpty)
        throw new RuntimeException("There are functional properties")

      lista.toArray(prop)
      val Length = Properties.count.asInstanceOf[Int]
      var domain = Array.ofDim[OWLIndividual](Length, Length)
      var dataPropertiesValue = Array.ofDim[OWLLiteral](Length, Length)

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
    } */

    /* def getDatatypeAssertions(dataProperty: OWLDataProperty): Map[OWLIndividual, Set[OWLLiteral]] = {

      val statements: Map[OWLIndividual, Set[OWLLiteral]] = new HashMap[OWLIndividual, Set[OWLLiteral]]()

      for (ex <- Examples) {
        val dataPropertyValues: Set[OWLLiteral] = reasoner.getDataPropertyValues(ex.asInstanceOf[OWLNamedIndividual], dataProperty)
        statements.put(ex, dataPropertyValues)
      }
      statements
    } */
  }
}
