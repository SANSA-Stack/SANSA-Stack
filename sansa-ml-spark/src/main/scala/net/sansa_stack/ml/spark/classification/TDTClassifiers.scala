package net.sansa_stack.ml.spark.classification

import java.util.ArrayList
import java.util.HashSet
import java.util.Iterator
import java.util.List
import java.util.Stack
import java.net.URI
import collection.JavaConverters._

import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLDataFactory
import org.semanticweb.owlapi.model.OWLIndividual
import org.semanticweb.owlapi.model.IRI
import net.sansa_stack.ml.spark.classification
import net.sansa_stack.ml.spark.classification.KB.KB
import net.sansa_stack.ml.spark.classification._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }

/*
 * Terminological Decision Tree Classifier
 */

object TDTClassifiers {

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

  class TDTClassifiers(var k: KB, var sc: SparkSession) {

    /**
     * TDT induction algorithm implementation
     *
     * @param prob Learning problem
     * @param father father concept
     * @param posExs positive examples
     * @param negExs negative examples
     * @param undExs unknown examples
     * @param nCandRefs
     * @param prPos
     * @param prNeg
     * @return
     */

    def induceDLTree(father: OWLClassExpression,
                     posExs: RDD[String], negExs: RDD[String], undExs: RDD[String],
                     nCandRefs: Int, prPos: Double, prNeg: Double): DLTree = {

      val THRESHOLD: Double = 0.05
      val tree: DLTree = new DLTree() // new (sub)tree		

      if (posExs.count.toInt == 0 && negExs.count.toInt == 0) // There is no examples
        if (prPos >= prNeg) { // prior majority of positives
          tree.setRoot(k.getDataFactory().getOWLThing()) // set positive leaf
          println("-----\nPostive leaf (prior1)")
          tree
        } else { // prior majority of negatives
          tree.setRoot(k.getDataFactory().getOWLNothing()) // set negative leaf
          println("-----\nNegative leaf (prior1)")
          tree
        }

      val numPos = posExs.count.toDouble
      val numNeg = negExs.count.toDouble
      val perPos: Double = numPos / (numPos + numNeg)
      val perNeg: Double = numNeg / (numPos + numNeg)

      println("new per Pos: " + perPos)
      println("new per Neg: " + perNeg)

      if (perNeg == 0 && perPos > THRESHOLD) { // no negative
        tree.setRoot(k.getDataFactory().getOWLThing()) // set positive leaf
        println("-----\nPostive leaf (prior2)")
        tree
      } else if (perPos == 0 && perNeg > THRESHOLD) { // no positive			
        tree.setRoot(k.getDataFactory().getOWLNothing()); // set negative leaf
        println("-----\nNegative leaf (prior2)\n");
        tree
      }

      //	else (a non-leaf node) ...

      // generate set of concepts
      val Con: RDD[OWLClassExpression] = generateRefs(k, father, nCandRefs, posExs, negExs)
      Con.take(50).foreach(println(_))

      // select best partitioning node concept
      val bestConcept: OWLClassExpression = selectBestConcept(k, Con, posExs, negExs, undExs, prPos, prNeg)

      val sNode = split(k, bestConcept, posExs, negExs, undExs)
      
      // set the root concept
      tree.setRoot(bestConcept.getNNF)
      
      println("\nTree: " + tree)
      
      // sNode._1._1 = PosEL, sNode._2._1 = NegEL, sNode._3._1 =  undEL             
      // sNode._1._2 = PosER, sNode._2._2 = NegER, sNode._3._2 =  undER     
       
      // build subtrees
      tree.setPosTree(induceDLTree(bestConcept, sNode._1._1, sNode._2._1, sNode._3._1, nCandRefs, perPos, perNeg))
      
      tree.setNegTree(induceDLTree(bestConcept.getComplementNNF, sNode._1._2, sNode._2._2, sNode._3._2, nCandRefs, perPos, perNeg))
      
     // println("tree: " + tree)
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

      if (r1 + r2 == 0) {
        val missingVForTDT = false

        if (missingVForTDT) {
          cP += classify(ind, tree.getPosSubTree)
          cn -= classify(ind, tree.getNegSubTree)
          // case of tie
          if (cP > (-1 * cn)) +1
          else if (cP < (-1 * cn)) -1
          else 0
        } else 0
      } else if (r1 * r2 == 1) r1
      else if ((r1 != 0)) r1
      else r2
    }
    
     /**
   * @param prob
   * @param concept
   * @param dim
   * @param posExs
   * @param negExs
   * @return
   */
  private def generateRefs(prob: KB, concept: OWLClassExpression, dim: Int, posExs: RDD[String],
                           negExs: RDD[String]): RDD[OWLClassExpression] = {

    println("\nGenerating node concepts: \n ")
    var rConcepts: Array[OWLClassExpression] = Array.ofDim[OWLClassExpression](dim)
    var newConcept: OWLClassExpression = null
    var refinement: OWLClassExpression = null
    var emptyIntersection: Boolean = false

    for (c <- 0 until dim) {

      do {
        emptyIntersection = false //true
        refinement = new RefinementOperator(prob).getRandomConcept(prob)
        val newConcepts: HashSet[OWLClassExpression] = new HashSet[OWLClassExpression]()

        if (concept != refinement)
        {
          newConcepts.add(concept)
          newConcepts.add(refinement)  
        }
        newConcept = prob.getDataFactory.getOWLObjectIntersectionOf(newConcepts)

        emptyIntersection = !prob.getReasoner.isSatisfiable(newConcept)
        //val iterator: Iterator[OWLIndividual] = prob.getReasoner().getInstances(newConcept, false).entities().iterator().asInstanceOf[Iterator[OWLIndividual]]
        //emptyIntersection = prob.getReasoner().getInstances(newConcept, false).entities().count() < 1
        //val nextInd : OWLIndividual = iterator.next()
        //nextInd.equals(obj)
        
        
      } while (emptyIntersection)
      rConcepts(c) = newConcept
    }
    var Refs: RDD[OWLClassExpression] = sparkSession.sparkContext.parallelize(rConcepts)
    var nRef = Refs.distinct().count.toInt
    println("No. of generated concepts: " + nRef)
    Refs.distinct()
  }
  
       
 //				while (emptyIntersection && instIterator.hasNext()) {
//					OWLIndividual nextInd = (OWLIndividual) instIterator.next();
//					int index = -1;
//					for (int i=0; index<0 && i<allIndividuals.length; ++i)
//						if (nextInd.equals(allIndividuals[i])) index = i;
//					if (posExs.contains(index))
//						emptyIntersection = false;
//					else if (negExs.contains(index))
//						emptyIntersection = false;
//				}					       
        
 
  
  
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

  def selectBestConcept(prob: KB, 
                        concepts: RDD[OWLClassExpression],
                        posExs: RDD[String],
                        negExs: RDD[String],
                        undExs: RDD[String],
                        prPos: Double, prNeg: Double): OWLClassExpression = {

    var bestConceptIndex: Int = 0

    println("\nThe First concept is: " + concepts.first())
    var counts: Array[Int] = getSplitCounts(prob, concepts.first(), posExs, negExs, undExs)

    println("\nPL:" +counts(0) +",\t NL:" + counts(1) + ",\t UL:" + counts(2) + ",\tPR:" + counts(3) + 
        ",\tNR:" + counts(4) + ",\tUR:" + counts(5))

    //var bestGain: Double = gain(counts,  prPos, prNeg)
    var bestGain: Double = gain(counts)
    //println("\nBest gain: "+ bestGain)

    for (c <- 1 until concepts.count.toInt) {

      var nConcept = concepts.take(concepts.count.toInt).apply(c)
      println("\nConcept " + c +" is: " + nConcept)

      counts = getSplitCounts(prob, nConcept, posExs, negExs, undExs)
      println("\nPL:" +counts(0) +",\t NL:" + counts(1) + ",\t UL:" + counts(2) + ",\tPR:" + counts(3) + 
        ",\tNR:" + counts(4) + ",\tUR:" + counts(5))

      // var thisGain: Double = gain(counts, prPos, prNeg)
      var thisGain: Double = gain(counts)
      println("\n Current gain: " + thisGain)

      if (thisGain > bestGain) {
        bestConceptIndex = c
        bestGain = thisGain
      }
    }

    println("\n --------\nBest gain: " + bestGain + " \t Split index: " + bestConceptIndex)

    val nCpt = concepts.take(concepts.count.toInt).apply(bestConceptIndex)
    println("\n Best concept is: " + nCpt)
    nCpt
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
                             posExs: RDD[String],
                             negExs: RDD[String],
                             undExs: RDD[String]): Array[Int] = {

    val counts: Array[Int] = Array.ofDim[Int](6)

    //val (PosEL, PosER) = splitGroup(prob, concept, posExs)
    //(NegEL, NegER) ; splitGroup(prob, concept, negExs)
    //(undEL, undER) ; splitGroup(prob, concept, undExs)
   
    val Pos = splitGroup(prob, concept, posExs)
    val Neg = splitGroup(prob, concept, negExs)
    val Und = splitGroup(prob, concept, undExs)

    counts(PL) = Pos._1.count.toInt
    counts(NL) = Neg._1.count.toInt
    counts(UL) = Und._1.count.toInt
    counts(PR) = Pos._2.count.toInt
    counts(NR) = Neg._2.count.toInt
    counts(UR) = Und._2.count.toInt

   /* for (i <- 0 until counts.length)
      println(counts(i))*/

    counts
  }

  /**
   * @param prob
   * @param concept
   * @param nodeExamples
   * @param leftExs
   * @param rightExs
   */
  private def splitGroup(prob: KB, concept: OWLClassExpression,
                         nodeExamples: RDD[String]): (RDD[String], RDD[String]) = {

    println("\nNode examples: \n ----------")
    nodeExamples.take(5).foreach(println(_))

    val negConcept: OWLClassExpression = prob.getDataFactory.getOWLObjectComplementOf(concept)
    var Left = new ArrayList[String]()
    var Right = new ArrayList[String]()

    //val ex = nodeExamples.zipWithIndex().map{case (x,y) => (y,x)}
    //val propName: RDD[String] = prob.getIndividuals().map( ind => ind.asOWLNamedIndividual().getIRI.getShortForm)

    for (e <- 0 until nodeExamples.count.toInt) {

      val nodeEx = nodeExamples.take(e + 1).apply(e)
      val nodeInd = prob.getDataFactory().getOWLNamedIndividual(nodeEx).asInstanceOf[OWLIndividual]
      // val exInd = sparkSession.sparkContext.parallelize(Array(nodeEx))

      if (prob.getReasoner().isEntailed(prob.getDataFactory.getOWLClassAssertionAxiom(concept, nodeInd))) {
        Left.add(nodeEx)
        println("first condition")
      } else if (prob.getReasoner().isEntailed(prob.getDataFactory.getOWLClassAssertionAxiom(negConcept, nodeInd))) {
        Right.add(nodeEx)
        println("second condition")
      } /*else {
        printf("last if\n")
        Left.add(nodeEx)
        Right.add(nodeEx)
      }*/
   }

    val leftRDD = sparkSession.sparkContext.parallelize(Left.asScala)
    val rightRDD = sparkSession.sparkContext.parallelize(Right.asScala)

    println("\nleft ex: ")
    leftRDD.take(20).foreach(println(_))

    println("\nright ex: ")
    rightRDD.take(20).foreach(println(_))
    
    (leftRDD, rightRDD)
    
    
     //  println("\n nodeEx = " + nodeEx )
      //val Filtered = prob.getIndividuals().filter(_ == nodeInd)
      // println("\n filtered = " )
      // Filtered.take(10).foreach(println(_))
      //val exIndex = ex.lookup(e)
      // println("the element: ")
      //exInd.take(1).foreach(println(_))    
      //val ind  = prob.getDataFactory().getOWLNamedIndividual(IRI.create(nodeEx)).asInstanceOf[OWLIndividual]
      //println("newexample  " + ind )

      //val x = prob.getIndividuals().take(nodeExamples.count.toInt).apply(e)
      //val x = prob.getIndividuals().filter( _ == neew).first()

      //x.take(20).foreach(println(_))

      //val r =prob.getReasoner().isEntailed(prob.getDataFactory.getOWLClassAssertionAxiom(concept, ind))
      //println("\n r = " + r)

      // val l =prob.getReasoner().isEntailed(prob.getDataFactory.getOWLClassAssertionAxiom(negConcept, ind))
      //println("\n l = " + l)
  }

  /**
   * @param prob
   * @param concept
   * @param posExs
   * @param negExs
   * @param undExs
   */

  private def split(prob: KB,
                    concept: OWLClassExpression,
                    posExs: RDD[String], negExs: RDD[String], undExs: RDD[String]):
                    ((RDD[String], RDD[String]), (RDD[String], RDD[String]), (RDD[String], RDD[String])) = {

   val Pos = splitGroup(prob, concept, posExs)
   val Neg = splitGroup(prob, concept, negExs)
   val Und = splitGroup(prob, concept, undExs)
   
   (Pos, Neg, Und)
  }

  }//class

 
  /**
   * @param counts
   * @param prPos
   * @param prNeg
   * @return The calculated Gain
   */

  /*
   * Function to calculate the gain
   */

  /*def gain(counts: Array[Int], prPos: Double, prNeg: Double): Double = {
    
    val Trsize: Double = counts(0) + counts(1)
    val Flsize: Double = counts(3) + counts(4)
    val Usize: Double = counts(2) + counts(5)// + counts(6) + counts(7)
    
    val size: Double =  Trsize + Flsize + Usize
    
    val startImpurity : Double = 	gini(counts(0) + counts(3), counts(1) + counts(4), prPos, prNeg)
     
    val TrImpurity = gini(counts(0), counts(1), prPos, prNeg)
    val FlImpurity = gini(counts(3), counts(4), prPos, prNeg)
    val UImpurity = gini(counts(2) + counts(6), counts(5) + counts(7), prPos, prNeg)
	  
    val Gainval = startImpurity - (Trsize/size)*TrImpurity - (Flsize/size)*FlImpurity - -(Usize/size)*UImpurity
	
    Gainval
  }
  
  def gini(nPos: Double, nNeg: Double, prPos: Double, prNeg: Double): Double = {
    
    val estimatProp : Int = 3
    val total: Double = nPos + nNeg
    
    val p1 : Double = (nPos*estimatProp*prPos)/(total+estimatProp)
    val p2: Double = (nNeg*estimatProp*prNeg)/(total+estimatProp)
    
    val ginival = 1.0-p1*p1-p2*p2
    ginival
  }*/

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
   * Selecting the best in a list (RDD) of refinements using Entropy information
   * @param prob
   * @param concepts
   * @param posExs
   * @param negExs
   * @param undExs
   * @param prPos
   * @param prNeg
   * @param truePosExs
   * @param trueNegExs
   * @return
   */

 /* def selectBestConceptEntropy(prob: KB, concepts: RDD[OWLClassExpression],
                               posExs: RDD[String],
                               negExs: RDD[String],
                               undExs: RDD[String],
                               prPos: Double, prNeg: Double,
                               truePosExs: RDD[String],
                               trueNegExs: RDD[String]): OWLClassExpression = {

    var bestConceptIndex: Int = 0
    var counts: Array[Int] = getSplitCounts(prob, concepts.first(), posExs, negExs, undExs)

    println("%4s\t p:%d n:%d u:%d\t p:%d n:%d u:%d\t p:%d n:%d u:%d\t ", "#" + 0,
      counts(0), counts(1), counts(2), counts(3), counts(4), counts(5), counts(6), counts(7), counts(8))

    var minEntropy: Double = Entropy(counts, prPos, prNeg, truePosExs.count.toInt, trueNegExs.count.toInt)
    println("%+10e\n" + minEntropy)
    println(concepts.first())

    val n = concepts.zipWithIndex().map { case (x, y) => (y, x) }
    for (c <- 1 until concepts.count.toInt) {
      val nConcept = n.lookup(c).asInstanceOf[OWLClassExpression]
      counts = getSplitCounts(prob, nConcept, posExs, negExs, undExs)
      println("%4s\t p:%d n:%d u:%d\t p:%d n:%d u:%d\t p:%d n:%d u:%d\t ", "#" + c,
        counts(0), counts(1), counts(2), counts(3), counts(4), counts(5), counts(6), counts(7), counts(8))

      val thisEntropy: Double = Entropy(counts, prPos, prNeg, truePosExs.count.toInt, trueNegExs.count.toInt)
      println("%+10e\n" + minEntropy)
      println(nConcept)

      if (thisEntropy < minEntropy) {
        bestConceptIndex = c;
        minEntropy = thisEntropy;
      }

    }
    println("\n -------- best gain: %f \t split #%d\n", minEntropy, bestConceptIndex)

    val nCpt = n.lookup(bestConceptIndex).asInstanceOf[OWLClassExpression]
    println("\n %s\n\n", nCpt)
    nCpt
  }*/

  /*
   * Function to calculate the Entropy value
   */

  def Entropy(counts: Array[Int], prPos: Double, prNeg: Double, sizPos: Int, sizNeg: Int): Double = {
    val nP = counts(0) + counts(1)
    val nN = counts(3) + counts(4)
    val nU = counts(2) + counts(5) + counts(6) + counts(7)
    val total = nP + nN + nU

    val c =
      if (total != 0)
        (nP + nN) / total
      else 0

    val sizeTP: Double = counts(0) + 1
    val sizeFP: Double = counts(1) + 1
    val sizeFN: Double = counts(3) + 1
    val sizeTN: Double = counts(4) + 1

    var Tpr: Double = 0
    if ((sizeTP + sizeFP) != 0)
      Tpr = (sizeTP) / (sizeTP + sizeFP)
    else
      Tpr = 1

    var Fpr =
      if ((sizeFP + sizeTN) != 0)
        (sizeFP + 0.5) / (sizeFP + sizeTN)
      else 1

    var p1 =
      if ((2 - Tpr - Fpr) != 0)
        (1 - Tpr) / (2 - Tpr - Fpr)
      else 1

    var p2 =
      if ((2 - Tpr - Fpr) != 0)
        (1 - Fpr) / (2 - Tpr - Fpr)
      else 1

    val EntropyValue: Double = (-(Tpr + Fpr) * ((Tpr / (Tpr + Fpr)) * Math.log(Tpr / (Tpr + Fpr)) - (Fpr / (Tpr + Fpr)) * Math.log(Fpr / (Tpr + Fpr)))
      - (2 - p1 - p2) * (p1 * Math.log(p1) - p2 * Math.log(p2)))

    EntropyValue
  }

}//object
