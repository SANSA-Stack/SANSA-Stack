package net.sansa_stack.ml.spark.classification.decisionTrees

import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.StructuralReasoner

import java.util
import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.StructuralReasoner.StructuralReasoner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.reasoner.OWLReasoner



object ConceptsGenerator{

  class ConceptsGenerator(protected var kb: KB) {


    
    /**
     *  A function to generate the query concepts
     *
     *  @param numConceptsToGenerate The number of concepts to be generated
     *  @return Array of the generated query concepts
     */
   
    def generateQueryConcepts(numConceptsToGenerate: Int): Array[OWLClassExpression] = {
  
      val reasoner: StructuralReasoner = kb.getReasoner
      val dataFactory: OWLDataFactory = kb.getDataFactory
      val allExamples: RDD[OWLIndividual] = kb.getIndividuals

      println("Generate Query Concepts\n-----------")
      val queryConcept: Array[OWLClassExpression] = Array.ofDim[OWLClassExpression](numConceptsToGenerate)

      val minOfSubConcepts: Int = 2
      val maxOfSubConcepts: Int = 8
      var numOfSubConcepts: Int = 2

      var i: Int = 0
      var j: Int = 0

      var nextConcept: OWLClassExpression = null
      var complementPartialConcept: OWLClassExpression = null
      val nEx : Int = allExamples.count().toInt

      // cycle to build numConceptsToGenerate new query concepts
      i = 0
      while (i < numConceptsToGenerate) {
        var partialConcept: OWLClassExpression = null

        numOfSubConcepts = minOfSubConcepts + kb.generator.nextInt(maxOfSubConcepts - minOfSubConcepts)

        var numPosInst: Int = 0
        var numNegInst: Int = 0

        // build a single new query OWLClassExpression adding conjuncts or disjunctions
        do {
          // take the first subConcept for building the query OWLClassExpression
          partialConcept = kb.getRandomConcept
          j = 1
          while (j < numOfSubConcepts) {

            val newConcepts = new util.HashSet[OWLClassExpression]()

            newConcepts.add(partialConcept)

            nextConcept = kb.getRandomConcept
            if (!newConcepts.contains(nextConcept)) newConcepts.add(nextConcept)
            else {
              nextConcept = kb.getRandomConcept
              newConcepts.add(nextConcept)
            }

            partialConcept =
              if (kb.generator.nextInt(4) == 0) {
                dataFactory.getOWLObjectIntersectionOf(newConcepts)
              }
              else dataFactory.getOWLObjectUnionOf(newConcepts)
            j = j + 1
          } // for j

          complementPartialConcept = dataFactory.getOWLObjectComplementOf(partialConcept)
  
          numPosInst = reasoner.getInstances(partialConcept, b = false).size()
          numNegInst = reasoner.getInstances(complementPartialConcept, b = false).size()

//          numPosInst = reasoner.getInstances(partialConcept, false).getFlattened.size()
//          numNegInst = reasoner.getInstances(complementPartialConcept, false).getFlattened.size()
          
          println()
          println(partialConcept)
          print("  pos: " + numPosInst)
          print("  neg: " + numNegInst)
          print("  und: " + (nEx - numNegInst - numPosInst) + "\n")
          println()

          //     partialConcept = kb.getRandomConcept

          /* println("pos:%d (%3.1f)\t\t neg:%d (%3.1f)\t\t und:%d (%3.1f)\n " + numPosInst + numPosInst * 100.0 / nExs,
            numNegInst, numNegInst * 100.0 / nExs,
            (nExs - numNegInst - numPosInst),
            (nExs - numNegInst - numPosInst) * 100.0 / nExs) */
          //      } while (!reasoner.isSatisfiable(partialConcept))
        } while (numPosInst + numNegInst == 0 || numPosInst + numNegInst == nEx)
        // (numPosInst * numNegInst == 0) || ((numPosInst < 10) || (numNegInst < 10))
   //   } while (!reasoner.isSatisfiable(partialConcept) || !reasoner.isSatisfiable(complementPartialConcept))

        // add the newly built OWLClassExpression to the list of all required query concepts
        queryConcept(i) = partialConcept
        println("Query " + (i + 1) + " found\n\n")

        i = i + 1
      }

      queryConcept
    }

  }
}
