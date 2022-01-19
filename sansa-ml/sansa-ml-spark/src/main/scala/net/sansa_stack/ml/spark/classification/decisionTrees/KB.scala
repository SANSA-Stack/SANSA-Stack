package net.sansa_stack.ml.spark.classification.decisionTrees

import collection.JavaConverters._
import java.io.File
// import java.io.Serializable
import java.util.Random
import java.util.stream.Collectors
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL.int2jvalue
import org.semanticweb.HermiT.Configuration
import org.semanticweb.HermiT.Reasoner
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.reasoner.OWLReasoner
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory
import org.semanticweb.owlapi.reasoner.structural.StructuralReasonerFactory
import org.semanticweb.owlapi.util.SimpleIRIMapper

import scala.collection.JavaConverters._
import scala.compat.java8.StreamConverters._

import net.sansa_stack.ml.spark.classification.decisionTrees._
/*
* The class to define the Knowledgebase elements
*/

class KB (var UrlOwlFile: String, rdd: OWLAxiomsRDD) extends Serializable {
  
  val d: Double = 0.3
  var generator: Random = new Random(2)
  
  var ontology: OWLOntology = initKB()
//  @transient var reasoner: OWLReasoner = _
//  @transient var hermit: Reasoner = _
  var manager: OWLOntologyManager = _
  var concepts: RDD[OWLClass] = _
  var dataFactory: OWLDataFactory = _
  var Examples: RDD[OWLIndividual] = _
  var objectProperties: RDD[OWLObjectProperty] = _
  
  //  Date property: property, values and domains
  var dataPropertiesValue: RDD[RDD[OWLLiteral]] = _
  var dataProperties: RDD[OWLDataProperty] = _
  var domain: RDD[OWLIndividual] = _
  //  var classifications: Array[Array[Int]] = _
  var classifications: RDD[((OWLClassExpression, OWLIndividual), Int)] = _
  var choiceDataP: Random = new Random(1)
  var choiceObjectP: Random = new Random(1)
  var cc: ((OWLClassExpression, OWLIndividual), Int) = _
  
  
  
  class KB(url: String) extends Serializable {
    UrlOwlFile = url
    ontology = initKB()
  }
  
  def initKB(): OWLOntology = {
    
    val owlFile: File = new File(UrlOwlFile)
    
    manager = OWLManager.createOWLOntologyManager()
    
    val ontology: OWLOntology = manager.loadOntologyFromOntologyDocument(owlFile)
    //    println("\nLoading ontology: \n----------------\n" + ontology)
    
    // obtain the location where the ontology was loaded from
    val iri = manager.getOntologyDocumentIRI(ontology)
    
    // Add mapping for the local ontology
    manager.getIRIMappers.add(new SimpleIRIMapper(iri, IRI.create(owlFile)))
    
    // The data factory provides a point for creating OWL API objects such as classes, properties and individuals.
    dataFactory = manager.getOWLDataFactory
    
   
    
    // --------- concepts Extraction -----------------
    
    concepts = rdd.flatMap {
      case axiom: HasClassesInSignature => axiom.classesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()
    
    //      println("\n\nConcepts\n-------\n")
    //      concepts.take(20).foreach(println(_))
    
    //     val nConcepts : Int = concepts.count.toInt
    //      println("\nNumber of concepts: " + nConcepts)
    
    // --------- Object Properties Extraction -----------------
    
    objectProperties = rdd.map {
      case axiom: HasProperty[OWLObjectProperty] => axiom.getProperty
      case _ => null
    }.filter(_ != null).distinct()
    
    //      println("\nObject Properties\n----------")
    //      objectProperties.take(10).foreach(println(_))
    
    //     val nObjProperties : Int = objectProperties.count.toInt
    //      println("\nNumber of object properties: " + nObjProperties)
    
    // -------- Data Properties Extraction --------
    
    dataProperties = rdd.flatMap {
      case axiom: HasDataPropertiesInSignature => axiom.dataPropertiesInSignature().iterator().asScala
      case _ => null
    }.filter(_ != null).distinct()
    
    //      println("\nData Properties\n----------")
    //      dataProperties.take(10).foreach(println(_))
    
    //     val nDataProperties : Int = dataProperties.count.toInt
    //    println("\nNumber of data properties: " + nDataProperties)
    
    // --------- Individual Extraction ------------
    
    Examples = rdd.flatMap {
      case axiom: HasIndividualsInSignature => axiom.individualsInSignature().collect(Collectors.toSet()).asScala
      case _ => null
    }.filter(null != _).distinct()
      .asInstanceOf[RDD[OWLIndividual]]
    
    //      println("\nIndividuals\n-----------")
    //      Examples.take(50).foreach(println(_))
    
    //      val nEx : Int = Examples.count.toInt
    //      println("\nNumber of Individuals: " + nEx)
    
    println("\nKB loaded. \n")
    ontology
  }
  
  def getObjectProperties: RDD[OWLObjectProperty] = objectProperties
  
  def getClasses: RDD[OWLClass] = concepts
  
  def getIndividuals: RDD[OWLIndividual] = Examples
  
  def getDataProperties: RDD[OWLDataProperty] = dataProperties
  
  //    def getDomains: Array[Array[OWLIndividual]] = domain
  
  def getDataFactory: OWLDataFactory = dataFactory
  
  def getOntology: OWLOntology = ontology
  
  def getReasoner: Reasoner = {
    // Reasoner configuration
    val con: Configuration = new Configuration()
    @transient val reasonerFactory: OWLReasonerFactory = new StructuralReasonerFactory()
  
    //  var reasoner: OWLReasoner = _
    //  @transient var hermit: Reasoner = _
    val reasoner = reasonerFactory.createReasoner(ontology)
  
    val hermit = new Reasoner(con, ontology) with Serializable
    
    hermit
  }
  
  def getClassMembershipResults: RDD[((OWLClassExpression, OWLIndividual), Int)] = classifications
  
  // def getURL(): String = urlOwlFile
  
  object KB extends Serializable {
  
  
    def getClassMembershipResult(testConcepts: Array[OWLClassExpression], negTestConcepts: Array[OWLClassExpression],
                                 examples: RDD[OWLIndividual]): RDD[((OWLClassExpression, OWLIndividual), Int)] = {
      
      println("\nClassifying all individuals \n ------------ ")
      
      println("Processed concepts (" + testConcepts.length + "): \n")
      
      val flag: Boolean = false
      
      for (c <- 0 until testConcepts.length) {
        
        classifications = examples.map(e => ((testConcepts(c), e), 0))
        
        classifications = classifications.mapPartitions { partition =>
  
          partition.map { cl =>
    
            if (getReasoner.isEntailed(dataFactory.getOWLClassAssertionAxiom(testConcepts(c), cl._1._2))) {
              ((testConcepts(c), cl._1._2), 1)
            }
            else {
              if (!flag) {
                if (getReasoner.isEntailed(dataFactory.getOWLClassAssertionAxiom(negTestConcepts(c), cl._1._2))) {
                  ((testConcepts(c), cl._1._2), -1)
                } else {
                  ((testConcepts(c), cl._1._2), 0)
                }
              } else {
                ((testConcepts(c), cl._1._2), -1)
              }
            }
          }
  
        }
        val elements = classifications.map(c => (c._2, 1)).reduceByKey(_ + _)
        elements.foreach(println(_))
      
        }
      classifications.cache() // The classifications rdd will be used frequently
      
      classifications
    }
  }
  
  
  //  def Print (data : String) : Unit = println(data)
  
  //    def setClassMembershipResult(classifications: Array[Array[Int]]): Unit = {
  //      this.classifications = classifications
  //    }
  
  //    def getDataPropertiesValue: RDD[RDD[OWLLiteral]] = dataPropertiesValue
  
  //   def getClassMembershipResult: Array[Array[Int]] = classifications
  
  //    def getRoleMembershipResult(objectProperties: RDD[OWLObjectProperty], examples: RDD[OWLIndividual]): RDD[Array[Array[Int]]] = {
  //
  //      println("\nVerifyng all individuals' relationship ------ ")
  //
  //      val x = objectProperties.count.asInstanceOf[Int]
  //      val y = examples.count.asInstanceOf[Int]
  //
  //      val Related: Array[Array[Array[Int]]] = Array.ofDim[Int](x, y, y)
  //      val RelatedRole = sparkSession.sparkContext.parallelize(Related)
  //
  //      for (i <- 0 until x; j <- 0 until y; k <- 0 until y) {
  //
  //        // verify that the example related to the example j k with respect to the rule
  //        var x = Related(i)(j)(k) = 0
  //
  //        RelatedRole.map { x =>
  //          x match {
  //            case a if (reasoner.isEntailed(dataFactory.getOWLObjectPropertyAssertionAxiom(objectProperties.map(i => i).asInstanceOf[OWLObjectProperty],
  //              examples.map(j => j).asInstanceOf[OWLIndividual], examples.map(k => k).asInstanceOf[OWLIndividual]))) => 1
  //            case _ => -1
  //
  //          }
  //        }
  //      } // for loop
  //
  //      RelatedRole
  //    }
  

  
  //    def getRandomProperty(numQueryProperty: Int): Array[Int] = {
  //
  //      val queryProperty: Array[Int] = Array.ofDim[Int](numQueryProperty)
  //      var dataTypeProperty: Int = 0
  //      while (dataTypeProperty < numQueryProperty) {
  //        val query: Int = choiceDataP.nextInt(dataProperties.count.asInstanceOf[Int])
  //        if (domain(query).length > 1) {
  //          // creation of dataProperty used for the test
  //          queryProperty(dataTypeProperty) = query
  //          dataTypeProperty += 1
  //
  //        }
  //      }
  //      queryProperty
  //    }
  
  //    def getRandomObjectProperties(numRules: Int): Array[Int] = {
  //      val RulesTest: Array[Int] = Array.ofDim[Int](numRules)
  //      for (i <- 0 until numRules)
  //        RulesTest(i) = choiceObjectP.nextInt(numRules)
  //      RulesTest
  //    }
  
  //    def updateExamples(individuals: RDD[OWLIndividual]): Unit = {
  //      Examples = individuals
  //    }
  
  def getRandomConcept: OWLClassExpression = {
    // randomly choose one of the concepts present
    var newConcept: OWLClassExpression = null
    
    val BinaryClassification = false
    if (!BinaryClassification) {
      do {
        // case A:  ALC and more expressive ontologies
        newConcept = concepts.takeSample(true, 1)(0)
        
        if (generator.nextDouble() < 0.7) {
          val newConceptBase: OWLClassExpression = getRandomConcept
          if (generator.nextDouble() < 0.1) {
            if (generator.nextDouble() < 0) { // new role restriction
              val role: OWLObjectProperty = objectProperties.takeSample(true, 1)(0)
              
              newConcept =
                if (generator.nextDouble() < 0.5) {
                  dataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
                }
                else {
                  dataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
                }
            } else {
              newConcept = dataFactory.getOWLObjectComplementOf(newConceptBase)
            }
          }
        }
      } while (!getReasoner.isSatisfiable(newConcept))
      //     newConcept
      
    } else {
      
      // for less expressive ontologies ALE and so on (only complement to atomic concepts)
      do {
        newConcept = concepts.takeSample(true, 1)(0)
        if (generator.nextDouble() < d) {
          val newConceptBase: OWLClassExpression = getRandomConcept
          if (generator.nextDouble() < d) {
            if (generator.nextDouble() < 0.1) { // new role restriction
              
              val role: OWLObjectProperty = objectProperties.takeSample(true, 1)(0)
              
              newConcept =
                if (generator.nextDouble() < d) {
                  dataFactory.getOWLObjectAllValuesFrom(role, newConceptBase)
                } else {
                  dataFactory.getOWLObjectSomeValuesFrom(role, newConceptBase)
                }
            }
          }
        } else {
          newConcept = dataFactory.getOWLObjectComplementOf(newConcept)
        }
      } while (!getReasoner.isSatisfiable(newConcept))
      
      //     newConcept
    }
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

//    def  getClassMembershipResult(testConcepts: Array[OWLClassExpression], negTestConcepts: Array[OWLClassExpression],
//                                 examples: RDD[OWLIndividual]): Array[Array[Int]] = {
//
//      println("\nClassifying all examples \n ------------ ")
//
//      val flag: Boolean = false
//      classifications = Array.ofDim[Int](testConcepts.length, examples.count.toInt)
//      println("Processed concepts (" + testConcepts.length + "): \n")
//      val r: Reasoner = getReasoner
//
//      for (c <- 0 until testConcepts.size) {
//        var p: Int = 0
//        var n: Int = 0
//        println("\nTest Concept number " + (c + 1) + ": " + testConcepts(c))
//
//        for (e <- 0 until examples.count.toInt) {
//
//          classifications(c)(e) = 0
//          val ind = examples.take(e + 1).apply(e)
//
//          if (r.isEntailed(getDataFactory.getOWLClassAssertionAxiom(testConcepts(c), ind))) {
//            classifications(c)(e) = +1
//            p = p + 1
//          }
//          else {
//            if (!flag) {
//              if (r.isEntailed(getDataFactory.getOWLClassAssertionAxiom(negTestConcepts(c), ind))) {
//                classifications(c)(e) = -1
//              }
//            } else {
//              classifications(c)(e) = -1
//            }
//
//            n = n + 1
//          }
//        }
//
//        println("\n Pos: " + p + "\t Neg: " + n)
//      }
//
//      classifications
//    }
