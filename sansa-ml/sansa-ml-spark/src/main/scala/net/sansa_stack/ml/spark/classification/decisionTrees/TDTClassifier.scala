package net.sansa_stack.ml.spark.classification.decisionTrees

import java.util

import net.sansa_stack.ml.spark.classification.decisionTrees.DLTree.DLTree
import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.EntitySearcher
import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.StructuralReasoner.StructuralReasoner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.semanticweb.owlapi.model._
import scala.collection.JavaConverters._
import scala.util.Random


/**
 * Terminological Decision Tree Classifier
 *
 * @author Heba Mohamed
 */

object TDTClassifier {
  
  /**
   * L for the left branch and R for the right branch
   * P, N, U for positive, negative and unlabeled respectively
   */
  
  val PL: Int = 0
  val NL: Int = 1
  val UL: Int = 2
  val PR: Int = 3
  val NR: Int = 4
  val UR: Int = 5
  
  class TDTClassifier2(var kb: KB) extends Serializable {
  
    var nodes: Int = 0
    val df: OWLDataFactory = kb.getDataFactory
    var refs: RDD[OWLClassExpression] = _
    val ro: RefinementOperator = new RefinementOperator(kb)
    val parallelism: Int = 30
    val reasoner: StructuralReasoner = kb.getReasoner
    val spark: SparkSession = kb.getSparkSession
  
    /**
     * TDT induction algorithm implementation
     *
     * @param father father concept
     * @param posExs positive examples
     * @param negExs negative examples
     * @param undExs unlabeled examples
     * @param nRefs  number of candidate references
     * @param prPos  prior majority positive examples
     * @param prNeg  prior majority negative examples
     * @return Learning Tree
     */

    def induceDLTree(father: OWLClassExpression,
                     posExs: RDD[OWLIndividual],
                     negExs: RDD[OWLIndividual],
                     undExs: RDD[OWLIndividual],
                     nRefs: Int,
                     prPos: Double,
                     prNeg: Double): DLTree = {
      
      println("\nLearning problem: p: " + posExs.count().toInt + "\t n: " + negExs.count().toInt
            + "\t u: " + undExs.count().toInt + "\t prPos: " + prPos + "\t prNeg: " + prNeg + "\n")
  
      val THRESHOLD: Double = 0.05
      val tree: DLTree = new DLTree()
     // var refinements: util.Set[OWLClassExpression] = new util.HashSet[OWLClassExpression]
  
      posExs.distinct(parallelism)
      negExs.distinct(parallelism)
      undExs.distinct(parallelism)
    
      if (posExs.count == 0 && negExs.count == 0) // There is no examples
        if (prPos >= prNeg) { // prior majority of positives
          tree.setRoot(df.getOWLThing) // set positive leaf
//          nodes += 1
          println("\nPositive leaf (prior1)")
          return tree
        } else { // prior majority of negatives
          tree.setRoot(df.getOWLNothing) // set negative leaf
//          nodes += 1
          println("\nNegative leaf (prior1)")
          return tree
        }
  
      val numPos = posExs.count.toDouble
      val numNeg = negExs.count.toDouble
  
      val total = numPos + numNeg
  
//      var perPos: Double = 0
//      var perNeg: Double = 0
  
//      if (total != 0) {
      val perPos = numPos / total
      val perNeg = numNeg / total
//      }
//      else {
//        return tree
//      }
  
//      println("new per Pos: " + f"$perPos%1.2f")
//      println("new per Neg: " + f"$perNeg%1.2f")
  
      if (perNeg == 0 && perPos > THRESHOLD) { // no negative
        tree.setRoot(df.getOWLThing) // set positive leaf
//        nodes += 1
        println("\nPositive leaf (prior2)")
        return tree
      }
      else if (perPos == 0 && perNeg > THRESHOLD) { // no positive
        tree.setRoot(df.getOWLNothing) // set negative leaf
//        nodes += 1
        println("\nNegative leaf (prior2)\n")
        return tree
      }
      //        else if (prNeg == perNeg || prPos == perPos) {
      //            tree.setRoot(df.getOWLThing)
      //            nodes += 1
      //            return tree
      //        }
   
      // (a non-leaf node)
      // generate a set of concepts
//      refs = refine1(kb, father, nRefs)
  
//      if (father.isOWLThing) {
//         println ("1st one")
//         refs = generateRefs(kb, father, nRefs)
//      }
//      else {
//        println("2nd one")
      val refinements = ro.getSubsumedConcept(father)
//      refinements = refine(father)
      refs = spark.sparkContext.parallelize(Seq(refinements))
                            .flatMap(a => a.asScala).distinct()
    
      println("\nrefinements are : ")
      refs.foreach(println(_))
      println()
//      }
  
      //           val refinements: RDD[OWLClassExpression] = generateRefs(kb, father, nRefs)
//      val tmpConcept = father
      // select best partitioning node concept
      val bestConcept: OWLClassExpression = selectBestConcept(kb, refs, posExs, negExs, undExs, prPos, prNeg)
  
//      if (bestConcept == tmpConcept)
//      {
//        tree.setRoot(bestConcept.getNNF)
//        tree
//
//      } else
//        if (!bestConcept.isOWLThing) {
//      if (bestConcept.isOWLNothing) {
//        tree.setRoot(df.getOWLNothing) // set negative leaf
//      } else if (bestConcept.isOWLThing) {
//        tree.setRoot(df.getOWLThing) // set positive leaf
// //      } else if (bestConcept == tmpConcept) {
// //        println("\nfather is the same as child")
// //        tree.setRoot(bestConcept.getNNF)
//      } else {
        val sNode = split(kb, bestConcept, posExs, negExs, undExs)
      
      // set the root concept
        tree.setRoot(bestConcept.getNNF)
    
        // sNode._1._1 = PosEL, sNode._2._1 = NegEL, sNode._3._1 =  undEL
        // sNode._1._2 = PosER, sNode._2._2 = NegER, sNode._3._2 =  undER
    
        // build subtrees
        println("\nStart Positive tree \n----------")
        tree.setPosTree(induceDLTree(bestConcept, sNode._1._1, sNode._2._1,
          sNode._3._1, nRefs, prPos, prNeg))
    
        println("\nStart Negative tree \n----------")
        tree.setNegTree(induceDLTree(bestConcept.getComplementNNF, sNode._1._2,
          sNode._2._2, sNode._3._2, nRefs, prPos, prNeg))
   
//     }

      tree
      
    }
 
    def refine (currentConcept: OWLClassExpression): util.Set[OWLClassExpression] = {
      
      var children: util.Set[_ <: OWLClassExpression] = null
//      val definition: OWLClassExpression = currentConcept
      var childrenList = new util.ArrayList[OWLClassExpression]()
      val searcher = new EntitySearcher(kb)
      var n: Int = -1
  
      if ((!currentConcept.isOWLThing) && (!currentConcept.isOWLNothing)) {
    
        val rand: Random = new Random()
        children = searcher.getChildren(currentConcept)
        println("\n children are: ")
        children.forEach(println(_))
    
        if (children.size()>0) {
          n = rand.nextInt(children.size())
          childrenList = new util.ArrayList[OWLClassExpression](children);
        }
      }
  
      val refineConcept: OWLClassExpression =
        if (n != -1) childrenList.get(n)
        else currentConcept
  
      val refine = ro.getSubsumedConcept(refineConcept)
      refine
    }
    /**
     * Function to generate the refinement concepts of the given one.
     * @param kb knowledgebase
     * @param currentConcept Father Concept
     * @param dim number of candidate references
     * @return RDD of the refinement concepts
     */
    
    
    def refine1(kb: KB, currentConcept: OWLClassExpression, dim: Int): RDD[OWLClassExpression] = {
  
      print("\nGenerating node concepts: ")
      
      val concepts: Array[OWLClassExpression] = Array.ofDim[OWLClassExpression](dim)
//      val concepts: util.HashSet[OWLClassExpression] = new util.HashSet[OWLClassExpression]()
      var refinements: util.Set[OWLClassExpression] = new util.HashSet[OWLClassExpression]
      var newConcept: OWLClassExpression = null
      var emptyIntersection: Boolean = false
      
      if (currentConcept.isOWLThing) {
          for (c <- 0 until dim) {
            do {
              newConcept = ro.getRandomConcept(kb)
              refinements.add(newConcept)
  
              if (newConcept != null ) {
                refinements.add(currentConcept)
                newConcept = df.getOWLObjectIntersectionOf(refinements)
              } else emptyIntersection = false
  
              if (newConcept != null) {
                emptyIntersection = !reasoner.isSatisfiable(newConcept)
              } else emptyIntersection = false
            } while (emptyIntersection)
  
            concepts(c) =
              if (newConcept != null) newConcept
              else currentConcept
  
            print(" " + c)
          }
          refs = spark.sparkContext.parallelize(concepts).distinct()
        
      } else {
            refinements = ro.getSubsumedConcept(currentConcept)
            refs = spark.sparkContext.parallelize(Seq(refinements))
                             .flatMap(a => a.iterator().asScala).distinct()
      }
      
      println("\n\nGenerated Refinements")
      refs.foreach(println(_))
      
      refs
    }
    
    
    /**
     * @param kb knowledgebase
     * @param concept Father Concept
     * @param dim number of candidate references
     * @return
     */
    private def generateRefs(kb: KB,
                             concept: OWLClassExpression,
                             dim: Int): RDD[OWLClassExpression] = {
  
      print("\nGenerating node concepts: ")
      val rConcepts: Array[OWLClassExpression] = Array.ofDim[OWLClassExpression](dim)
      var newConcept: OWLClassExpression = null
      var refinement: OWLClassExpression = null
      var emptyIntersection: Boolean = false
  
      for (c <- 0 until dim) {
        do {
           val Concepts = new util.HashSet[OWLClassExpression]()
      
            refinement = new RefinementOperator(kb).getSubsumedRandomConcept(concept)
      
          if (refinement != null && !Concepts.contains(refinement) ) { // && !refinement.isInstanceOf[OWLObjectAllValuesFrom]
            Concepts.add(concept)
            Concepts.add(refinement)
            newConcept = kb.getDataFactory.getOWLObjectIntersectionOf(Concepts)
          }
          else emptyIntersection = false
      
          if (newConcept != null) {
            emptyIntersection = !reasoner.isSatisfiable(newConcept)
          } else emptyIntersection = false
      
        } while (emptyIntersection)
    
        rConcepts(c) =
          if (newConcept != null) newConcept
          else concept
    
        print(" " + c)
    
      }
      println()
      val Refs: RDD[OWLClassExpression] = spark.sparkContext.parallelize(rConcepts).distinct()
      //      val nRef = Refs.distinct().count
      //      println("\nNo. of generated concepts: " + nRef)
      Refs
    }
  
  
  
    /**
     * Selecting the best in a list (RDD) of refinements
     * @param kb Knowledgebase
     * @param concepts RDD of concepts
     * @param posExs RDD of positive examples
     * @param negExs RDD of negative examples
     * @param undExs RDD of undefined examples
     * @param prPos prior majority of positive examples
     * @param prNeg prior majority of negative examples
     * @return the best concept
     */
    
    def selectBestConcept(kb: KB,
                          concepts: RDD[OWLClassExpression],
                          posExs: RDD[OWLIndividual],
                          negExs: RDD[OWLIndividual],
                          undExs: RDD[OWLIndividual],
                          prPos: Double, prNeg: Double): OWLClassExpression = {
      
      var bestConceptIndex: Int = 0
      val noOfConcepts = concepts.count.toInt
      val conceptArray: Array[OWLClassExpression] = concepts.take(noOfConcepts)
      var counts: Array[Int] = Array.ofDim[Int](6)
  
      var bestGain: Double = 0.0
      var thisGain: Double = 0.0
  
  
      if (noOfConcepts == 0) {
        df.getOWLThing
      } else {
        counts = getSplitCounts(kb, concepts.first(), posExs, negExs, undExs)
  
        bestGain = Entropy(counts)
//       bestGain = gain(counts)
//        bestGain = gain(counts, prPos, prNeg)
  
        println("\n#0    " + " \tPL:" + counts(0) + ",\t NL:" + counts(1) + ",\t UL:" + counts(2) + ",\tPR:" + counts(3) +
          ",\tNR:" + counts(4) + ",\tUR:" + counts(5) + ",\t gain: " + f"$bestGain%8.4f")
      
  
        //      val newConcepts = concepts.map { c =>
        //        counts = getSplitCounts(kb, c, posExs, negExs, undExs)
        //        val thisGain: Double = gain(counts)
        //
        //        if (thisGain > bestGain) {
        //          bestGain = thisGain
        //        }
        //
        //        println("#" + c + "  \tPL:" +counts(0) +",\t NL:" + counts(1) + ",\t UL:" + counts(2) + ",\tPR:" + counts(3) +
        //          ",\tNR:" + counts(4) + ",\tUR:" + counts(5) + ",\t gain: " + f"$thisGain%8.3f")
        //
        //        (c, thisGain)
        //      }
  
//        if (noOfConcepts > 10) {
//          noOfConcepts = 10
//        }
        for (c <- 1 until noOfConcepts) {
    
          val nConcept: OWLClassExpression = concepts.take(c + 1).apply(c)
          counts = getSplitCounts(kb, nConcept, posExs, negExs, undExs)
    
           thisGain = Entropy(counts)
//            thisGain = gain(counts)
//          thisGain = gain(counts, prPos, prNeg)
  
          if (thisGain > bestGain) {
            bestConceptIndex = c
            bestGain = thisGain
          }
    
          println("#" + c + "  \tPL:" + counts(0) + ",\t NL:" + counts(1) + ",\t UL:" + counts(2) + ",\tPR:" + counts(3) +
            ",\tNR:" + counts(4) + ",\tUR:" + counts(5) + ",\t gain: " + f"$thisGain%8.4f")
        }

      }

     
//      if (bestGain != 0) {
  
        println("\n --------\nBest gain: " + f"$bestGain%8.4f " + ", \t Splitting Index: " + bestConceptIndex)
//          + ", \t PL:" + counts(0) +
//          ",\t NL:" + counts(1) + ",\t UL:" + counts(2) + ",\tPR:" + counts(3) +
//          ",\tNR:" + counts(4) + ",\tUR:" + counts(5))

        val nCpt = conceptArray.apply(bestConceptIndex)
    
        println("\nBest concept is: " + nCpt)
        nCpt
//      }
//    else {
//        println("\nAll Gains equal zero return")
//        df.getOWLThing
//      }
     
   }
  


    /**
     * Function to calculate the gain based on gini index
     *
     * @param counts
     * @param prPos
     * @param prNeg
     * @return The calculated Gain based on gini index
     */
    def gain(counts: Array[Int], prPos: Double, prNeg: Double): Double = {

      val tSize: Double = counts(0) + counts(3)
      val fSize: Double = counts(1) + counts(4)
      val uSize: Double = counts(2) + counts(5)

      val size: Double = tSize + fSize + uSize

      val startImpurity: Double = gini(tSize, fSize, prPos, prNeg)

      val tImpurity = gini(counts(0), counts(1), prPos, prNeg)
      val fImpurity = gini(counts(3), counts(4), prPos, prNeg)
      val uImpurity = gini(counts(2), counts(5), prPos, prNeg)

      var gainVal = startImpurity - (tSize / size) * tImpurity - (fSize / size) * fImpurity - (-(uSize/size)) * uImpurity
      
      if (gainVal.isNaN) {
        gainVal = 0.0
      }
      
      gainVal
    }

    def gini(nPos: Double, nNeg: Double, prPos: Double, prNeg: Double): Double = {

      val estimatProp : Int = 3
      val total: Double = nPos + nNeg

      val p1: Double = (nPos * estimatProp * prPos) / (total + estimatProp)
      val p2: Double = (nNeg * estimatProp * prNeg) / (total + estimatProp)

      1.0-p1*p1-p2*p2
      
    }

    /**
   * Function to calculate the Entropy value
   */
  
     def Entropy(counts: Array[Int]): Double = {
       val nP = counts(0) + counts(1)
       val nN = counts(3) + counts(4)
       val nU = counts(2) + counts(5) // + counts(6) + counts(7)
       val total = nP + nN + nU
  
       val c =
         if (total != 0) {
           (nP + nN) / total
         } else 0
  
       val sizeTP: Double = counts(0) + 1
       val sizeFP: Double = counts(1) + 1
       val sizeFN: Double = counts(3) + 1
       val sizeTN: Double = counts(4) + 1
  
       var Tpr: Double = 0
       if ((sizeTP + sizeFP) != 0) {
         Tpr = sizeTP / (sizeTP + sizeFP)
       } else Tpr = 1
  
       val Fpr =
         if ((sizeFP + sizeTN) != 0) {
           (sizeFP + 0.5) / (sizeFP + sizeTN)
         } else 1
  
       val p1 =
         if ((2 - Tpr - Fpr) != 0) {
           (1 - Tpr) / (2 - Tpr - Fpr)
         } else 1
  
       val p2 =
         if ((2 - Tpr - Fpr) != 0) {
           (1 - Fpr) / (2 - Tpr - Fpr)
         } else 1
  
       val EntropyValue: Double = (-(Tpr + Fpr) * ((Tpr / (Tpr + Fpr)) * Math.log(Tpr / (Tpr + Fpr)) - (Fpr / (Tpr + Fpr)) * Math.log(Fpr / (Tpr + Fpr)))
         - (2 - p1 - p2) * (p1 * Math.log(p1) - p2 * Math.log(p2)))
  
       EntropyValue
     }
    
    /** Function to calculate the gain
     * @param counts, array contains the number of +ve, -ve and uncertain examples for each branch
     * @return The calculated Gain
     */
    
    private def gain(counts: Array[Int]): Double = {
      
      var gain: Double = 0.0
      val totalL: Double = counts(PL) + counts(NL) + 0.001
      val totalR: Double = counts(PR) + counts(NR) + 0.001
      val total: Double = totalL + totalR
      val pPL: Double = counts(PL) / totalL
      val pPR: Double = counts(PR) / totalR
      val pNL: Double = counts(NL) / totalL
      val pNR: Double = counts(NR) / totalR
      
      if (Math.abs(pPL + pPR) != 0 && Math.abs(pNL + pNR) != 0 )
      {
        gain = (totalL / total) * (totalR / total) *
          Math.pow(Math.abs(pPL - pPR) / Math.abs(pPL + pPR) + Math.abs(pNL - pNR) / Math.abs(pNL + pNR), 2)
        
//        gain = round(gain * 1000) / 1000.0
      }
  
      
      gain
      
    }
    
    /**
     * @param kb knowledgebase
     * @param concept father concept
     * @param posExs RDD of positive examples
     * @param negExs RDD of negative examples
     * @param undExs RDD of undefined examples
     * @return
     */
    
    private def getSplitCounts(kb: KB,
                               concept: OWLClassExpression,
                               posExs: RDD[OWLIndividual],
                               negExs: RDD[OWLIndividual],
                               undExs: RDD[OWLIndividual]): Array[Int] = {
      
      val counts: Array[Int] = Array.ofDim[Int](6)
      
      if (!concept.isOWLThing && concept != null) {
        if (posExs.count() != 0.0) {
          val Pos = splitGroup(kb, concept, posExs)
//          if (!Pos._1.isEmpty())
            counts(PL) = Pos._1.count.toInt
//          if (!Pos._2.isEmpty())
            counts(PR) = Pos._2.count.toInt
        }
  
        if (negExs.count() != 0.0) {
          val Neg = splitGroup(kb, concept, negExs)
//          if (!Neg._1.isEmpty())
            counts(NL) = Neg._1.count.toInt
//          if (!Neg._1.isEmpty())
            counts(NR) = Neg._2.count.toInt
         }
  
        if (undExs.count() != 0.0) {
          val Und = splitGroup(kb, concept, undExs)
//          if (!Und._1.isEmpty())
            counts(UL) = Und._1.count.toInt
//          if (!Und._1.isEmpty())
            counts(UR) = Und._2.count.toInt
        }
       counts
       
      } else counts
    }
    
    /**
     * @param kb knowledgebase
     * @param concept Concept
     * @param nodeExamples RDD of node examples
     */
    private def splitGroup(kb: KB,
                           concept: OWLClassExpression,
                           nodeExamples: RDD[OWLIndividual]): (RDD[OWLIndividual], RDD[OWLIndividual]) = {
      
   
      nodeExamples.persist(StorageLevel.MEMORY_AND_DISK)
      val negConcept: OWLClassExpression = df.getOWLObjectComplementOf(concept)
 
      val leftRDD = nodeExamples.filter { indv =>
        val nodeInd = df.getOWLNamedIndividual(indv.asOWLNamedIndividual())
//        kb.getReasoner.isEntailed(df.getOWLClassAssertionAxiom(concept, nodeInd))
        reasoner.isEntailed(df.getOWLClassAssertionAxiom(concept, nodeInd))
      }
      
      val rightRDD = nodeExamples.filter { indv =>
        val nodeInd = df.getOWLNamedIndividual(indv.asOWLNamedIndividual())
//        kb.getReasoner.isEntailed(df.getOWLClassAssertionAxiom(negConcept, nodeInd))
        reasoner.isEntailed(df.getOWLClassAssertionAxiom(negConcept, nodeInd))
     }
      
      nodeExamples.unpersist()
      (leftRDD, rightRDD)
    }
    
    /**
     * @param kb     , knowledge base
     * @param concept, the splitting concept
     * @param posExs , rdd of positive examples
     * @param negExs , rdd of negative examples
     * @param undExs , rdd of uncertain examples
     */
    
    private def split(kb: KB,
                      concept: OWLClassExpression,
                      posExs: RDD[OWLIndividual], negExs: RDD[OWLIndividual], undExs: RDD[OWLIndividual]):
    ((RDD[OWLIndividual], RDD[OWLIndividual]), (RDD[OWLIndividual], RDD[OWLIndividual]), (RDD[OWLIndividual], RDD[OWLIndividual])) = {
      
      val Pos = splitGroup(kb, concept, posExs)
      val Neg = splitGroup(kb, concept, negExs)
      val Und = splitGroup(kb, concept, undExs)
      
      (Pos, Neg, Und)
    }
  
    /**
     * recursive down through the tree modeSplit index
     * @param ind given individual
     * @param tree Decision Learning Tree
     * @return
     */
  
    def classify(ind: OWLIndividual, tree: DLTree): Int = {
      
      val missingValueTreatmentForTDT: Boolean = true
      
      val rootClass: OWLClassExpression = tree.getRoot
      val negRootClass: OWLClassExpression = df.getOWLObjectComplementOf(rootClass)
    
      if (rootClass.equals(df.getOWLThing)) return 1
      if (rootClass.equals(df.getOWLNothing)) return -1
    
      var r1: Int = 0
      var r2: Int = 0
//      var result: Int = 0
      if (reasoner.isEntailed(df.getOWLClassAssertionAxiom(rootClass, ind))) {
//        println("\nrootclass")
        r1 = classify(ind, tree.getPosSubTree)
      }
      else {
        if (reasoner.isEntailed(df.getOWLClassAssertionAxiom(negRootClass, ind))) {
//          println("\nneg rootclass")
          r2 = classify(ind, tree.getNegSubTree)
        }
      }
//        else {
//          r1 = 0
//          r2 = 0
//        }
  
      var cP: Int = 0
      var cN: Int = 0
      if ((r1 + r2) == 0) {
        if (missingValueTreatmentForTDT) {
          cP = cP + classify(ind, tree.getPosSubTree)
          cN = cP - classify(ind, tree.getNegSubTree)
          if (cP > (-1 * cN)) {
//            println("case 1")
            1}
          else if (cP < (-1 * cN)) -1
          else 0
        }
        else 0
      }
      else if (r1 * r2 == 1) {
//        println("case 2")
        r1
      }
      else {
        if (r1 != 0) {
//          println("case 3")
          r1}
        else r2
      }
  
    }
    
  } // class
} // object
