package net.sansa_stack.ml.spark.classification

import java.util.HashSet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.semanticweb.HermiT.Reasoner
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLDataFactory
import org.semanticweb.owlapi.model.OWLIndividual
import org.semanticweb.owlapi.model.OWLNamedIndividual
import net.sansa_stack.ml.spark.classification.KB.KB

object ConceptsGenerator{
  class ConceptsGenerator(protected var kb: KB) {

    protected var reasoner: Reasoner = kb.getReasoner
    protected var dataFactory: OWLDataFactory = kb.getDataFactory
    protected var allExamples: RDD[OWLIndividual] = kb.getIndividuals
  
    def generateQueryConcepts(numConceptsToGenerate: Int, sc: SparkSession): Array[OWLClassExpression] = {
      
      println("\nConcepts Generation\n-----------\n")
      val queryConcept: Array[OWLClassExpression] = Array.ofDim[OWLClassExpression](numConceptsToGenerate)
      val minOfSubConcepts: Int = 2
      val maxOfSubConcepts: Int = 8
      var numOfSubConcepts: Int = 0
      var i: Int = 0
      var j: Int = 0
      var nextConcept: OWLClassExpression = null
      var complPartialConcept: OWLClassExpression = null
      var nEx : Int = allExamples.count().toInt
      // cycle to build numConceptsToGenerate new query concepts
      i = 0
      while (i < numConceptsToGenerate) {
        var partialConcept: OWLClassExpression = null
        numOfSubConcepts = minOfSubConcepts + KB.generator.nextInt(maxOfSubConcepts - minOfSubConcepts)
        var numPosInst: Int = 0
        var numNegInst: Int = 0
  
        // build a single new query OWLClassExpression adding conjuncts or disjuncts
        do {
  
          //take the first subConcept for builiding the query OWLClassExpression
          partialConcept = kb.getRandomConcept
          //println("partial concept" + partialConcept)
          j = 1
                   
          while (j < numOfSubConcepts) {
            val newConcepts: HashSet[OWLClassExpression] = new HashSet[OWLClassExpression]()
            newConcepts.add(partialConcept)
            
            nextConcept = kb.getRandomConcept
            newConcepts.add(nextConcept)
           
            partialConcept =
              if (KB.generator.nextInt(4) == 0)
                dataFactory.getOWLObjectIntersectionOf(newConcepts)
              else dataFactory.getOWLObjectUnionOf(newConcepts) 
            j+=1
          } // for j
          println()
          complPartialConcept = dataFactory.getOWLObjectComplementOf(partialConcept)
          //println("\n", complPartialConcept)
          numPosInst = reasoner.getInstances(partialConcept, false).entities().count().toInt
          numNegInst = reasoner.getInstances(complPartialConcept, false).entities().count().toInt
         
          println("\n", partialConcept)
          println ("\n pos: " + numPosInst )
          println ("\n neg: " + numNegInst )
          println ("\n und: " + (nEx - numNegInst - numPosInst) )
          println()
          
          /*println("pos:%d (%3.1f)\t\t neg:%d (%3.1f)\t\t und:%d (%3.1f)\n " + numPosInst + numPosInst * 100.0 / nExs, 
            numNegInst, numNegInst * 100.0 / nExs,
            (nExs - numNegInst - numPosInst),
            (nExs - numNegInst - numPosInst) * 100.0 / nExs)*/
        } while ( ((numPosInst < 20) || (numNegInst > 3)))
  // (numPosInst * numNegInst == 0) || ((numPosInst < 10) || (numNegInst <10))
        
        //add the newly built OWLClassExpression to the list of all required query concepts
        queryConcept(i) = partialConcept
        //println("\n", partialConcept)
        println("Query " + (i+1) + " found\n\n")
        i+=1
      }
  
     queryConcept
    }
  
  }
}
