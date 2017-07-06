package net.sansa_stack.ml.spark.classification

import java.util.ArrayList
import java.util.HashSet
import java.util.Iterator
import java.util.List
import java.util.Stack
import collection.JavaConverters._

import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLDataFactory
import org.semanticweb.owlapi.model.OWLIndividual
import org.semanticweb.HermiT
import net.sansa_stack.ml.spark.classification
import net.sansa_stack.ml.spark.classification.KB.KB
import net.sansa_stack.ml.spark.classification._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/*
 * Terminological Decision Tree Classifier
 */

object TDTClassifier {

  /* L for the left branch and R for the right branch
   * P, N, U for postive, negative and unlabeled respectively
   */
  
  val PL: Int = 0
  val NL: Int = 1
  val UL: Int = 2
  val PR: Int = 3
  val NR: Int = 4
  val UR: Int = 5

    val sparkSession = SparkSession.builder
          .master("local[*]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .appName("TDT Classifier")
          .getOrCreate()
          
    val conf = new SparkConf().setAppName("TDTClassifier").setMaster("local[*]")  // local mode
    val sc = new SparkContext(conf)
  
 /**
   * Selecting the best in a list (RDD) of refinements
   * @param prob
   * @param concepts
   * @param posExs
   * @param negExs
   * @param undExs
   * @param prPos
   * @param prNeg
   * @return
   */
  
  def selectBestConcept(prob: KB, concepts: RDD[OWLClassExpression],
                                  posExs: RDD[Integer],
                                  negExs: RDD[Integer],
                                  undExs: RDD[Integer],
                                  prPos: Double, prNeg: Double): OWLClassExpression = {

    var bestConceptIndex: Int = 0

    var counts: Array[Int] = getSplitCounts(prob, concepts.first(), posExs, negExs, undExs)
    
    println("%4s\tp:%4d\t n:%4d\t u:%4d\t --- p:%4d\t n:%4d\t u:%4d\t ", "#" + 0,
     counts(PL), counts(NL), counts(UL), counts(PR), counts(NR), counts(UR))

    var bestGain: Double = gain(counts)
    println("%10f\n", bestGain)

    val n = concepts.zipWithIndex().map{case (x,y) => (y,x)}
        
    for (c <- 1 until concepts.count.asInstanceOf[Int]) {
      
      val nConcept = n.lookup(c).asInstanceOf[OWLClassExpression]
      
      counts = getSplitCounts(prob, nConcept, posExs, negExs, undExs)
      println("%4s\tp:%4d\t n:%4d\t u:%4d\t --- p:%4d\t n:%4d\t u:%4d\t ", "#" + c,
          counts(PL), counts(NL), counts(UL), counts(PR), counts(NR), counts(UR))

      val thisGain: Double = gain(counts)
      println("%10f\n", thisGain)
      
      if (thisGain > bestGain) {
        bestConceptIndex = c
        bestGain = thisGain
      }
    }

    println("\n -------- best gain: %f \t split #%d\n", bestGain, bestConceptIndex)
    
    val nCpt = n.lookup(bestConceptIndex).asInstanceOf[OWLClassExpression]
    println("\n %s\n\n", nCpt)
    nCpt
  }

  /**
   * @param counts
   * @return
   */
  
  /*
   * Function to calculate the gain
   */
  
  private def gain(counts: Array[Int]): Double = {

    val totalL: Double = counts(PL) + counts(NL) + 0.001
    val totalR: Double = counts(PR) + counts(NR) + 0.001
    val total: Double = totalL + totalR
    val pPL: Double = counts(PL) / totalL
    val pPR: Double = counts(PR) / totalR
    val pNL: Double = counts(NL) / totalL
    val pNR: Double = counts(NR) / totalR

    (totalL / total) * (totalR / total) *
      Math.pow(Math.abs(pPL - pPR) / Math.abs(pPL + pPR) + Math.abs(pNL - pNR) / Math.abs(pNL + pNR), 2)
  }

  /**
   * @param prob
   * @param concept
   * @param posExs
   * @param negExs
   * @param undExs
   * @return
   */
  
  
  private def getSplitCounts(prob: KB,
                             concept: OWLClassExpression,
                             posExs: RDD[Integer],
                             negExs: RDD[Integer],
                             undExs: RDD[Integer]): Array[Int] = {

    val counts: Array[Int] = Array.ofDim[Int](6)
    
    val posExsL: ArrayList[Integer] = new ArrayList[Integer]()
    val PosEL = sc.parallelize(posExsL.asScala)
    
    val negExsL: ArrayList[Integer] = new ArrayList[Integer]()
    val NegEL = sc.parallelize(negExsL.asScala)
    
    val undExsL: ArrayList[Integer] = new ArrayList[Integer]()
    val undEL = sc.parallelize(undExsL.asScala)

    val posExsR: ArrayList[Integer] = new ArrayList[Integer]()
    val PosER = sc.parallelize(posExsR.asScala)
    
    val negExsR: ArrayList[Integer] = new ArrayList[Integer]()
    val NegER = sc.parallelize(negExsR.asScala)
    
    val undExsR: ArrayList[Integer] = new ArrayList[Integer]()
    val undER = sc.parallelize(undExsR.asScala)
    
    splitGroup(prob, concept, posExs, PosEL, PosER)
    splitGroup(prob, concept, negExs, NegEL, NegER)
    splitGroup(prob, concept, undExs, undEL, undER)
    
    counts(PL) = PosEL.count.asInstanceOf[Int]
    counts(NL) = NegEL.count.asInstanceOf[Int]
    counts(UL) = undEL.count.asInstanceOf[Int]
    counts(PR) = PosER.count.asInstanceOf[Int]
    counts(NR) = NegER.count.asInstanceOf[Int]
    counts(UR) = undER.count.asInstanceOf[Int]
    counts
  }

  /**
   * @param prob
   * @param concept
   * @param posExs
   * @param negExs
   * @param undExs
   * @param posExsL
   * @param negExsL
   * @param undExsL
   * @param posExsR
   * @param negExsR
   * @param undExsR
   */
  private def split(prob: KB,
                    concept: OWLClassExpression,
                    posExs: RDD[Integer],
                    negExs: RDD[Integer],
                    undExs: RDD[Integer],
                    posExsL: RDD[Integer],
                    negExsL: RDD[Integer],
                    undExsL: RDD[Integer],
                    posExsR: RDD[Integer],
                    negExsR: RDD[Integer],
                    undExsR: RDD[Integer]): Unit = {
    
    splitGroup(prob, concept, posExs, posExsL, posExsR)
    splitGroup(prob, concept, negExs, negExsL, negExsR)
    splitGroup(prob, concept, undExs, undExsL, undExsR)
  }

  /**
   * @param prob
   * @param concept
   * @param nodeExamples
   * @param leftExs
   * @param rightExs
   */
  private def splitGroup(prob: KB,
                         concept: OWLClassExpression,
                         nodeExamples: RDD[Integer],
                         leftExs: RDD[Integer],
                         rightExs: RDD[Integer]): Unit = {

    val negConcept: OWLClassExpression = prob.getDataFactory.getOWLObjectComplementOf(concept)
    
    val ex = nodeExamples.zipWithIndex().map{case (x,y) => (y,x)}

    for (e <- 0 until nodeExamples.count.asInstanceOf[Int]) {

      val exIndex = ex.lookup(e)
      val exInd = sc.parallelize(exIndex)
       
      if (prob.getReasoner.isEntailed(prob.getDataFactory
                   .getOWLClassAssertionAxiom(concept, prob.getIndividuals().map{exIndex => exIndex}.asInstanceOf[OWLIndividual]))) 
          leftExs.union(exInd)
      else if (prob.getReasoner.isEntailed(prob.getDataFactory
                  .getOWLClassAssertionAxiom(negConcept, prob.getIndividuals().map{exIndex => exIndex}.asInstanceOf[OWLIndividual]))) 
          rightExs.union(exInd)
      else {
        leftExs.union(exInd)
        rightExs.union(exInd)
      }
    }
  }

    /**
   * @param prob
   * @param concept
   * @param dim
   * @param posExs
   * @param negExs
   * @return
   */
  private def generateRefs(prob: KB, concept: OWLClassExpression, dim: Int, posExs: RDD[Integer],
                           negExs: RDD[Integer]): Array[OWLClassExpression] = {

    println("\nGenerating node concepts ")
    var rConcepts: Array[OWLClassExpression] = Array.ofDim[OWLClassExpression](dim)
    var newConcept: OWLClassExpression = null
    var refinement: OWLClassExpression = null
    var emptyIntersection: Boolean = false

    for (c <- 0 until dim) {
      do {
        emptyIntersection = false //true
        refinement = new RefinementOperator(prob).getRandomConcept(prob)
        val newConcepts: HashSet[OWLClassExpression] = new HashSet[OWLClassExpression]()
        newConcepts.add(concept)
        newConcepts.add(refinement)
        newConcept = prob.getDataFactory.getOWLObjectIntersectionOf(newConcepts)

        emptyIntersection = !prob.getReasoner.isSatisfiable(newConcept)

      } while (emptyIntersection)
      rConcepts(c) = newConcept
      println("%d ", c)
    }
    println()
    rConcepts
  }


  

class TDTClassifier(var k: KB) {

  
  /**
   * TDT induction algorithm implementation
   *
   * @param prob Learning problem
   * @param father father concept
   * @param posExs positive examples
   * @param negExs negative examples
   * @param undExs unknown m. examples
   * @param nCandRefs
   * @param prPos
   * @param prNeg
   * @return
   */
  
  
  def induceDLTree(father: OWLClassExpression,
                   posExs: RDD[Integer], negExs: RDD[Integer],
                   undExs: RDD[Integer], nCandRefs: Int,
                   prPos: Double, prNeg: Double): DLTree = {

    val THRESHOLD: Double = 0.05

    println("\n * Learning problem\t p:%d\t n:%d\t u:%d\t prPos:%4f\t prNeg:%4f\n", posExs.count, negExs.count, undExs.count, prPos, prNeg)

    val tree: DLTree = new DLTree() // new (sub)tree		

    if (posExs.count == 0 && negExs.count == 0)        // There is no examples
      if (prPos >= prNeg) {                             // prior majority of positives
        tree.setRoot(k.getDataFactory().getOWLThing()) // set positive leaf
        println("-----\nPostive leaf (prior)")
        tree
      } 
      else {                                              // prior majority of negatives
        tree.setRoot(k.getDataFactory().getOWLNothing()) // set negative leaf
        println("-----\nNegative leaf (prior)")
        tree
      }

    val numPos: Double = posExs.count
    val numNeg: Double = negExs.count
    val perPos: Double = numPos / (numPos + numNeg)
    val perNeg: Double = numNeg / (numPos + numNeg)

    if (perNeg == 0 && perPos > THRESHOLD) {          // no negative
      tree.setRoot(k.getDataFactory().getOWLThing()) // set positive leaf
      println("-----\nPostive leaf (prior)")
      tree
    } 
    else if (perPos == 0 && perNeg > THRESHOLD) { // no positive			
      tree.setRoot(k.getDataFactory().getOWLNothing()); // set negative leaf
      println("-----\nNegative leaf (thr)\n");
      tree;
    }

    //	else (a non-leaf node) ...

    val cConcepts: Array[OWLClassExpression] = generateRefs(k, father, nCandRefs, posExs, negExs)
    val Con = sc.parallelize(cConcepts)
    
    // select node concept
    val bestConcept: OWLClassExpression = selectBestConcept(k, Con, posExs, negExs, undExs, prPos, prNeg)
    
    val posExsL: ArrayList[Integer] = new ArrayList[Integer]()
    val PosEL = sc.parallelize(posExsL.asScala)
    
    val negExsL: ArrayList[Integer] = new ArrayList[Integer]()
    val NegEL = sc.parallelize(negExsL.asScala)
     
    val undExsL: ArrayList[Integer] = new ArrayList[Integer]()
    val undEL = sc.parallelize(undExsL.asScala)
    
    val posExsR: ArrayList[Integer] = new ArrayList[Integer]()
    val PosER = sc.parallelize(posExsR.asScala)
    
    val negExsR: ArrayList[Integer] = new ArrayList[Integer]()
    val NegER = sc.parallelize(negExsR.asScala)
    
    val undExsR: ArrayList[Integer] = new ArrayList[Integer]()
    val undER = sc.parallelize(undExsR.asScala)
    

    split(k, bestConcept, posExs, negExs, undExs, PosEL, NegEL, undEL, PosER, NegER, undER)

    // select node concept
    tree.setRoot(bestConcept.getNNF)
    
    // build subtrees
    tree.setPosTree(induceDLTree(bestConcept, PosEL, NegEL, undEL, nCandRefs, prPos, prNeg))
    tree.setNegTree(induceDLTree(bestConcept.getComplementNNF, PosER, NegER, undER, nCandRefs, prPos, prNeg))
    tree

    
  }


  
  /**
   * recursive down through the tree model
   * @param ind
   * @param tree
   * @return
   */
  def classify(ind: OWLIndividual, tree: DLTree): Int = {
    
      val rootClass: OWLClassExpression = tree.getRoot
      if (rootClass == k.getDataFactory.getOWLThing) +1
      if (rootClass == k.getDataFactory.getOWLNothing) -1
      
      var r1: Int = 0
      var r2: Int = 0
      
      if (k.getReasoner.isEntailed(k.getDataFactory.getOWLClassAssertionAxiom(rootClass, ind)))
        r1 = classify(ind, tree.getPosSubTree)
      else if (k.getReasoner.isEntailed(k.getDataFactory.getOWLClassAssertionAxiom(k.getDataFactory.getOWLObjectComplementOf(rootClass), ind)))
        r2 = classify(ind, tree.getNegSubTree)
      
      var cP: Int = 0
      var cn: Int = 0
      
      if (r1 + r2 == 0){
        val missingVForTDT = false
        
        if (missingVForTDT) {
          cP += classify(ind, tree.getPosSubTree)
          cn -= classify(ind, tree.getNegSubTree)
          // case of tie
          if (cP > (-1 * cn)) +1 
          else if (cP < (-1 * cn)) -1 
          else 0
        } else 0
      }else if (r1 * r2 == 1) r1
      else if ((r1 != 0)) r1
      else r2
    }
  }
}
