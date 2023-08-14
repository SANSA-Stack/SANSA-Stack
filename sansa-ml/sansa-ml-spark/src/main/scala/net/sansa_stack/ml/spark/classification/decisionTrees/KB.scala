package net.sansa_stack.ml.spark.classification.decisionTrees


import java.io.File
import java.util.Random

import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.{EntitySearcher, StructuralReasoner}
import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.StructuralReasoner.StructuralReasoner
import net.sansa_stack.owl.spark.owlAxioms
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import net.sansa_stack.owl.spark.stats.OWLStats
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.HermiT.{Configuration, Reasoner}
import org.semanticweb.HermiT.ReasonerFactory
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.reasoner.OWLReasoner
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory
import org.semanticweb.owlapi.util.SimpleIRIMapper
import scala.jdk.CollectionConverters.asScalaIteratorConverter

/**
* The class to define the Knowledgebase elements
*/

class KB (var UrlOwlFile: String, rdd: OWLAxiomsRDD, spark: SparkSession) extends Serializable {
  
  
  val generator: Random = new Random(2)
  val ontology: OWLOntology = initKB()
  var dataFactory: OWLDataFactory = _
  var concepts: RDD[OWLClass] = _
  var individuals: RDD[OWLIndividual] = _
  var objectProperties: RDD[OWLObjectProperty] = _
  var dataProperties: RDD[OWLDataProperty] = _
  var objectPropertyAssertions: RDD[OWLObjectPropertyAssertionAxiom] = _
  var dataPropertyAssertions: RDD[OWLDataPropertyAssertionAxiom] = _
  var classifications: RDD[((OWLClassExpression, OWLIndividual), Int)] = _
  val searcher: EntitySearcher = new EntitySearcher(this)
  val reasoner: StructuralReasoner = new StructuralReasoner.StructuralReasoner(this)

//  class KB(url: String) extends Serializable {
//    UrlOwlFile = url
//    ontology = initKB()
//  }
  
  def initKB(): OWLOntology = {
  
    val parallelism: Int = 30     // Degree of parallelism
    val stats = new OWLStats(spark)
  
//    val owlFile: File = new File(UrlOwlFile)
  
    val manager = OWLManager.createOWLOntologyManager()
  
//    val ontology = manager.loadOntologyFromOntologyDocument(owlFile)
//
//    // obtain the location where the ontology was loaded from
//    val iri = manager.getOntologyDocumentIRI(ontology)
//
//    // Add mapping for the local ontology
//    manager.getIRIMappers.add(new SimpleIRIMapper(iri, IRI.create(owlFile)))
  
    // The data factory provides a point for creating OWL objects such as classes, properties and individuals.
    dataFactory = manager.getOWLDataFactory

    // --------- concepts Extraction -----------------
    concepts = stats.getClasses(rdd)
    
//      rdd.flatMap {
//      case axiom: HasClassesInSignature => axiom.classesInSignature().iterator().asScala
//      case _ => null
//    }.filter(_ != null).distinct(parallelism)
    
//   println("\n\nConcepts\n-------\n")
//   concepts.foreach(println(_))
    
//     val nConcepts: Int = concepts.count.toInt
//     println("\nNumber of concepts: " + nConcepts)
    
    // --------- Object Properties Extraction -----------------
    
    objectProperties = stats.getObjectProperties(rdd)
    
//      rdd.flatMap {
//      case axiom: HasObjectPropertiesInSignature => axiom.getObjectPropertiesInSignature.iterator().asScala
//      case _ => null
//    }.filter(_ != null).distinct(parallelism)
    
   
    // -------- Data Properties Extraction --------
    
    dataProperties = stats.getDataProperties(rdd)
    
    //    rdd.flatMap {
//      case axiom: HasDataPropertiesInSignature => axiom.dataPropertiesInSignature().iterator().asScala
//      case _ => null
//    }.filter(_ != null).distinct(parallelism)
    
 
    // --------- Individual Extraction ------------
    
    individuals = rdd.flatMap {
      case axiom: HasIndividualsInSignature => axiom.individualsInSignature().iterator().asScala
      case _ => null
    }.filter(null != _).distinct(parallelism)
      .asInstanceOf[RDD[OWLIndividual]]
  
  
    dataPropertyAssertions = owlAxioms.extractAxioms(rdd, AxiomType.DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLDataPropertyAssertionAxiom]]
  
    objectPropertyAssertions = owlAxioms.extractAxioms(rdd, AxiomType.OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLObjectPropertyAssertionAxiom]]
    
    println("\nKB loaded. \n")
    ontology
  }
  
  def isSatisfiable(cls: OWLClassExpression): Boolean = {

    !cls.isAnonymous &&
      !searcher.getEquivalentClasses(cls.asOWLClass()).collect().toList.contains(dataFactory.getOWLNothing)

  }
// def isSatisfiable(cls: OWLClassExpression): Boolean = {
//
//  !cls.isAnonymous &&
//    !searcher.getEquivalentClasses(cls).filter(c => c.isOWLNothing).isEmpty()
//
// }
  
  
  def getObjectProperties: RDD[OWLObjectProperty] = objectProperties
  
  def getClasses: RDD[OWLClass] = concepts
  
  def getIndividuals: RDD[OWLIndividual] = individuals
  
  def getDataProperties: RDD[OWLDataProperty] = dataProperties
  
  //    def getDomains: Array[Array[OWLIndividual]] = domain
  
  def getSubClassesAxioms: RDD[OWLAxiom] = {
    val subClasses = owlAxioms.extractAxioms(rdd, AxiomType.SUBCLASS_OF)
    
    subClasses
  }
  
  def getSubObjectPropertyAxioms: RDD[OWLAxiom] = {
    val subObject = owlAxioms.extractAxioms(rdd, AxiomType.SUB_OBJECT_PROPERTY)
  
    subObject
  }
  
  def getDataPropertyAssertions: RDD[OWLDataPropertyAssertionAxiom] = dataPropertyAssertions
  
  def getNegativeDataPropertyAssertion: RDD[OWLNegativeDataPropertyAssertionAxiom] = {
    val negDataPropertyAssertion = owlAxioms.extractAxioms(rdd, AxiomType.NEGATIVE_DATA_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeDataPropertyAssertionAxiom]]
    
    negDataPropertyAssertion
  }
  
  def getObjectPropertyAssertions: RDD[OWLObjectPropertyAssertionAxiom] = objectPropertyAssertions
  
  def getNegativeObjectPropertyAssertion: RDD[OWLNegativeObjectPropertyAssertionAxiom] = {
    val negDataPropertyAssertion = owlAxioms.extractAxioms(rdd, AxiomType.NEGATIVE_OBJECT_PROPERTY_ASSERTION)
      .asInstanceOf[RDD[OWLNegativeObjectPropertyAssertionAxiom]]
    
    negDataPropertyAssertion
  }
  
  
  def getClassAssertions: RDD[OWLClassAssertionAxiom] = {
    val classAssertions = owlAxioms.extractAxioms(rdd, AxiomType.CLASS_ASSERTION)
      .asInstanceOf[RDD[OWLClassAssertionAxiom]]
  
    classAssertions
  }
  
  def getSameIndividuals: RDD[OWLSameIndividualAxiom] = {
    val sameIndividuals = owlAxioms.extractAxioms(rdd, AxiomType.SAME_INDIVIDUAL)
      .asInstanceOf[RDD[OWLSameIndividualAxiom]]
  
    sameIndividuals
  }
  
  def getDifferentIndividuals: RDD[OWLDifferentIndividualsAxiom] = {
    val differentIndividuals = owlAxioms.extractAxioms(rdd, AxiomType.DIFFERENT_INDIVIDUALS)
      .asInstanceOf[RDD[OWLDifferentIndividualsAxiom]]
  
    differentIndividuals
  }
  
  
//  def classAssertionBC: Broadcast[Map[OWLIndividual, OWLClassExpression]] = {
//    val classAssertionMap = getClassAssertions.map(a => (a.getIndividual, a.getClassExpression)).collect().toMap
//    spark.sparkContext.broadcast(classAssertionMap)
//  }

  def getAxioms: OWLAxiomsRDD = rdd
  
  def getDataFactory: OWLDataFactory = dataFactory
  
  def getSparkSession: SparkSession = spark
  
  def getSparkContext: SparkContext = spark.sparkContext
  
  def getReasoner: StructuralReasoner = reasoner
  
  
//  @deprecatedOverriding
//  def getReasoner: OWLReasoner = {
//    // Reasoner configuration
// //    val con: Configuration = new Configuration()
// //    val hermit = new Reasoner(con, ontology) with Serializable
//
//    val reasonerFactory: OWLReasonerFactory = new ReasonerFactory ()
//    val hermit = reasonerFactory.createReasoner(ontology)
//
//    hermit
//  }
  
//  def getClassMembershipResults: RDD[((OWLClassExpression, OWLIndividual), Int)] = classifications
  
  // def getURL(): String = urlOwlFile
  
  object KB extends Serializable {
  
    def getClassMembershipResult(testConcepts: Array[OWLClassExpression],
                                 negTestConcepts: Array[OWLClassExpression],
                                 examples: RDD[OWLIndividual]): RDD[((OWLClassExpression, OWLIndividual), Int)] = {
    
 
      //      val flag: Boolean = false
      println("Classifying all individuals \n ------------ ")
      println("Processed concepts (" + testConcepts.length + "): \n")
    
      for (c <- testConcepts.indices) {
 
        classifications = examples
          //          .mapPartitions { partition =>
          //            partition
          .map { e =>
            if (getReasoner.isEntailed(getDataFactory.getOWLClassAssertionAxiom(testConcepts(c), e))) {
              ((testConcepts(c), e), 1)
            }
            else if (getReasoner.isEntailed(getDataFactory.getOWLClassAssertionAxiom(negTestConcepts(c), e))) {
              ((testConcepts(c), e), -1)
            }
            else {
              ((testConcepts(c), e), 0)
            }
          }
      
        //        classifications = examples.map(e => ((testConcepts(c), e), 0))
        //
        //        classifications = classifications
        // //          .mapPartitions { partition =>
        // //            partition
        //              .map { cl =>
        //
        //            if (getReasoner.isEntailed(dataFactory.getOWLClassAssertionAxiom(testConcepts(c), cl._1._2))) {
        //              ((testConcepts(c), cl._1._2), 1)
        //            }
        //            else if (getReasoner.isEntailed(dataFactory.getOWLClassAssertionAxiom(negTestConcepts(c), cl._1._2))) {
        //                ((testConcepts(c), cl._1._2), -1)
        //            }
        //            else {
        //                ((testConcepts(c), cl._1._2), 0)
        //            }
        // //            if (!flag) {
        // //                if (getReasoner.isEntailed(dataFactory.getOWLClassAssertionAxiom(negTestConcepts(c), cl._1._2))) {
        // //                  ((testConcepts(c), cl._1._2), -1)
        // //                }
        // //                else {
        // //                  ((testConcepts(c), cl._1._2), 0)
        // //                }
        // //            } else {
        // //                ((testConcepts(c), cl._1._2), -1)
        // //            }
        //            }
      
        //          }.coalesce(1)
      
        //        val elements = classifications.map(c => (c._2, 1)).reduceByKey(_ + _)
        //        elements.foreach(println(_))
      
      }
      //      classifications.foreach(println(_))
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
  
  //    def updateindividuals(individuals: RDD[OWLIndividual]): Unit = {
  //      Examples = individuals
  //    }
  
  def getRandomConcept: OWLClassExpression = {
  
    val d: Double = 0.3
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
        //      } while (!getReasoner.isSatisfiable(newConcept))
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
//      } while (!getReasoner.isSatisfiable(newConcept))
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

      for (ex <- individuals) {
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
