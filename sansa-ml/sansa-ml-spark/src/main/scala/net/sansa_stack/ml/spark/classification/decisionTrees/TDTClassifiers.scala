package net.sansa_stack.ml.spark.classification.decisionTrees

import collection.JavaConverters._
import java.util.ArrayList
import java.util.HashSet
import net.sansa_stack.ml.spark.classification.decisionTrees.KB
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model._
import scala.util.control.Breaks._


/*
 * Terminological Decision Tree Classifier
 */

object TDTClassifiers {

  /* L for the left branch and R for the right branch
   * P, N, U for positive, negative and unlabeled respectively
   */

  val PL: Int = 0
  val NL: Int = 1
  val UL: Int = 2
  val PR: Int = 3
  val NR: Int = 4
  val UR: Int = 5

  class TDTClassifiers(var k: KB, var sc: SparkSession) {

    /**
      * TDT induction algorithm implementation
      *
//      * @param know Learning knowledge
      * @param father father concept
      * @param posExs positive examples
      * @param negExs negative examples
      * @param undExs unlabeled examples
      * @param nRefs number of candidate references
      * @param prPos prior majority positive examples
      * @param prNeg prior majority negative examples
      * @return
      */

    def induceDLTree(father: OWLClassExpression,
                     posExs: RDD[String], negExs: RDD[String], undExs: RDD[String],
                     nRefs: Int, prPos: Double, prNeg: Double): DLTree = {

      val THRESHOLD: Double = 0.05
      val tree: DLTree = new DLTree()

      if (posExs.count == 0 && negExs.count == 0) // There is no examples
        if (prPos >= prNeg) { // prior majority of positives
          tree.setRoot(k.getDataFactory.getOWLThing) // set positive leaf
          println("-----\nPositive leaf (prior1)")
          return tree
        } else { // prior majority of negatives
          tree.setRoot(k.getDataFactory.getOWLNothing) // set negative leaf
          println("-----\nNegative leaf (prior1)")
          return tree
        }

      val numPos = posExs.count.toDouble
      val numNeg = negExs.count.toDouble

      val total = numPos + numNeg

      var perPos: Double = 0
      var perNeg: Double = 0

      if (total != 0) {
        perPos = numPos / total
        perNeg = numNeg / total
      }
      else {
        return tree
      }
      println("\nnew per Pos: " + perPos)
      println("new per Neg: " + perNeg)

      if (perNeg == 0 && perPos > THRESHOLD) { // no negative
        tree.setRoot(k.getDataFactory.getOWLThing) // set positive leaf
        println("-----\nPositive leaf (prior2)")
        return tree
      }
      else if (perPos == 0 && perNeg > THRESHOLD) { // no positive
        tree.setRoot(k.getDataFactory.getOWLNothing) // set negative leaf
        println("-----\nNegative leaf (prior2)\n")
        return tree
      }

      // else (a non-leaf node)
      // generate set of concepts
      val Con: RDD[OWLClassExpression] = generateRefs(k, father, nRefs, posExs, negExs)
      Con.take(50).foreach(println(_))

      // select best partitioning node concept
      val bestConcept: OWLClassExpression = selectBestConcept(k, Con, posExs, negExs, undExs, prPos, prNeg)

      if (bestConcept != null) {

        val sNode = split(k, bestConcept, posExs, negExs, undExs)

        // set the root concept
        tree.setRoot(bestConcept.getNNF)

        // sNode._1._1 = PosEL, sNode._2._1 = NegEL, sNode._3._1 =  undEL
        // sNode._1._2 = PosER, sNode._2._2 = NegER, sNode._3._2 =  undER


        // build subtrees

        println("\nStart Positive tree \n----------")
        tree.setPosTree(induceDLTree(bestConcept, sNode._1._1, sNode._2._1, sNode._3._1, nRefs, prPos, prNeg))

        println("\nStart Negative tree \n----------")
        tree.setNegTree(induceDLTree(bestConcept.getComplementNNF, sNode._1._2, sNode._2._2, sNode._3._2, nRefs, prPos, prNeg))

        tree
      }
      else {
        null
      }
    }

    /**
      * recursive down through the tree model
      * @param ind given individual
      * @param tree Decision Learning Tree
      * @return
      */
    def classify(ind: OWLIndividual, tree: DLTree): Int = {

      val rootClass: OWLClassExpression = tree.getRoot()
      println("\nrootClass " + rootClass)

      val negRootClass: OWLClassExpression = k.getDataFactory.getOWLObjectComplementOf(rootClass)
      println("negRootClass " + negRootClass)

      if (rootClass.equals(k.getDataFactory.getOWLThing)) return +1
      if (rootClass.equals(k.getDataFactory.getOWLNothing)) return -1

      var r1: Int = 0
      var r2: Int = 0

      if (k.getReasoner.isEntailed(k.getDataFactory.getOWLClassAssertionAxiom(rootClass, ind))) {
        r1 = classify(ind, tree.getPosSubTree())
      }
      else if (k.getReasoner.isEntailed(k.getDataFactory.getOWLClassAssertionAxiom(negRootClass, ind))) {
        r2 = classify(ind, tree.getNegSubTree())
      }

      var cP: Int = 0
      var cn: Int = 0

      if (r1 + r2 == 0) {
        val missingVForTDT = false

        if (missingVForTDT) {
          cP += classify(ind, tree.getPosSubTree())
          cn -= classify(ind, tree.getNegSubTree())

          if (cP > (-1 * cn)) +1
          else if (cP < (-1 * cn)) -1
          else 0
        } else 0
      } else if (r1 * r2 == 1) r1
      else if (r1 != 0) r1
      else r2
    }


    /**
      * @param know Knowledgebase
      * @param concept Father Concept
      * @param dim number of candidate references
      * @param posExs RDD of positive examples
      * @param negExs RDD of negative examples
      * @return
      */
    private def generateRefs(know: KB,
                             concept: OWLClassExpression,
                             dim: Int,
                             posExs: RDD[String],
                             negExs: RDD[String]): RDD[OWLClassExpression] = {

      println("\nGenerating node concepts: \n ")
      val rConcepts: Array[OWLClassExpression] = Array.ofDim[OWLClassExpression](dim)
      var newConcept: OWLClassExpression = null
      var refinement: OWLClassExpression = null
      var emptyIntersection: Boolean = false

      // val conceptExp = concept.nestedClassExpressions.iterator().asScala.toArray
  //    val C = concept.asConjunctSet()
    //  val ConceptExp = concept.asConjunctSet().iterator().asScala.toSeq
      // println("\nconcept set   " + C )

      for (c <- 0 until dim) {

        do {
          emptyIntersection = false // true
          val Concepts: HashSet[OWLClassExpression] = new HashSet[OWLClassExpression]()

          if (concept.equals(know.getDataFactory.getOWLThing)) {
            refinement = new RefinementOperator(know).getRandomConcept(know)
       //     println("\nrefinement " + c + " is " + refinement)
          }
          else {
            refinement = new RefinementOperator(know).getSubsumedRandomConcept(concept)
          }

          if (refinement != null && !Concepts.contains(refinement) && !refinement.isInstanceOf[OWLObjectAllValuesFrom]) {
            Concepts.add(concept)
            Concepts.add(refinement)
            newConcept = know.getDataFactory.getOWLObjectIntersectionOf(Concepts)
          }
          else emptyIntersection = false



          /* val con: OWLEquivalentClassesAxiom = know.dataFactory.getOWLEquivalentClassesAxiom(concept)

           val conExp: Array[OWLClassExpression] = con.classExpressions.iterator().asScala.toArray
           println("Concept Expressions = "  )
           conExp.foreach(println(_)) */

//          val refInstance: Boolean = refinement.isInstanceOf[OWLObjectAllValuesFrom]
//          breakable{
//
//            for (i <- ConceptExp)
//            {
//              if (i.isInstanceOf[OWLObjectSomeValuesFrom]) {
//                val y: OWLObjectSomeValuesFrom = i.asInstanceOf[OWLObjectSomeValuesFrom]
//                val conprop: OWLObjectProperty = y.getProperty.getNamedProperty
//                val confiller : OWLClassExpression = y.getFiller
//                /* println("============================")
//                 println("concept property = " + conprop)
//                 println("concept filler = " + confiller) */
//
//                if (refInstance) {
//                  val x : OWLObjectAllValuesFrom = refinement.asInstanceOf[OWLObjectAllValuesFrom]
//                  val rprop: OWLObjectProperty = x.getProperty.getNamedProperty
//                  val rfiller: OWLClassExpression = x.getFiller
//                   println("refinement property = " + rprop)
//                   println("refinement filler = " + rfiller)
//                  if (conprop == rprop) break
//
//                }
//              }
//            }
//            if (!ConceptExp.contains(refinement))
//            {
//              Concepts.add(concept)
//              Concepts.add(refinement)
//              newConcept = know.getDataFactory.getOWLObjectIntersectionOf(Concepts)
//              if (newConcept != null) {
//                emptyIntersection = !know.getReasoner.isSatisfiable(newConcept)
//              }
//              else emptyIntersection = false
//            }
//          }
      //    println("\n*****new concept " + newConcept)
          if (newConcept != null) {
                 emptyIntersection = !know.getReasoner.isSatisfiable(newConcept)
          }
          else emptyIntersection = false

        } while (emptyIntersection )

        rConcepts(c) =
          if (newConcept != null) newConcept
          else concept

      }
      val Refs: RDD[OWLClassExpression] = sc.sparkContext.parallelize(rConcepts)
      val nRef = Refs.distinct().count
      println("\nNo. of generated concepts: " + nRef)
      Refs.distinct()
    }

    // val iterator: Iterator[OWLIndividual] = know.getReasoner().getInstances(newConcept, false).entities().iterator().asInstanceOf[Iterator[OWLIndividual]]
    // val nextInd : OWLIndividual = iterator.next()

    /**
      * Selecting the best in a list (RDD) of refinements
      * @param know Knowledgebase
      * @param concepts RDD of concepts
      * @param posExs RDD of positive examples
      * @param negExs RDD of negative examples
      * @param undExs RDD of undefined examples
      * @param prPos prior majority of positive examples
      * @param prNeg prior majority of negative examples
      * @return the best concept
      */

    def selectBestConcept(know: KB,
                          concepts: RDD[OWLClassExpression],
                          posExs: RDD[String],
                          negExs: RDD[String],
                          undExs: RDD[String],
                          prPos: Double, prNeg: Double): OWLClassExpression = {

      var bestConceptIndex: Int = 0

      println("\nThe First concept is: " + concepts.first())
      var counts: Array[Int] = getSplitCounts(know, concepts.first(), posExs, negExs, undExs)

      println("\nPL:" +counts(0) +",\t NL:" + counts(1) + ",\t UL:" + counts(2) + ",\tPR:" + counts(3) +
        ",\tNR:" + counts(4) + ",\tUR:" + counts(5))

      // var bestGain: Double = gain(counts,  prPos, prNeg)
      var bestGain: Double = gain(counts)
      println("\nCurrent gain: " + bestGain)

      for (c <- 1 until concepts.count.toInt) {

        var nConcept = concepts.take(concepts.count.toInt).apply(c)
        println("\nConcept " + (c + 1) + " is: " + nConcept)

        counts = getSplitCounts(know, nConcept, posExs, negExs, undExs)
        println("\nPL:" +counts(0) +",\t NL:" + counts(1) + ",\t UL:" + counts(2) + ",\tPR:" + counts(3) +
          ",\tNR:" + counts(4) + ",\tUR:" + counts(5))

        // var thisGain: Double = gain(counts, prPos, prNeg)
        var thisGain: Double = gain(counts)
        println("\nCurrent gain: " + thisGain)

        if (thisGain > bestGain) {
          bestConceptIndex = c
          bestGain = thisGain
        }
      }

      val nCpt = concepts.take(concepts.count.toInt).apply(bestConceptIndex)

      if (bestGain == 0.0)  {
        null
        //      val parts = nCpt.nestedClassExpressions.iterator().asScala.toList
        //      val ref = parts.last
        //      val x = parts.filterNot(elem => elem == ref)
        //      println("refienment removed: ")
        //      x.foreach(println(_))
        //      var y: ArrayList[OWLClassExpression] = new ArrayList()
        //      var i = 0
        //      while (i< x.size)
        //      {
        //        val z = x.get(i)
        //        y.add(z)
        //        i = i+1
        //      }
        //
        //      nCpt
      }
      else {
        println("\n --------\nBest gain: " + bestGain + " \t Split index: " + bestConceptIndex)
        println("\nPL:" +counts(0) +",\t NL:" + counts(1) + ",\t UL:" + counts(2) + ",\tPR:" + counts(3) +
          ",\tNR:" + counts(4) + ",\tUR:" + counts(5))

        println("\n Best concept is: " + nCpt)
        nCpt
      }
    }

    /**
      * @param counts
      * @return The calculated Gain
      */

    /*
     * Function to calculate the gain
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
      }

      gain

    }
//
//
//    /**
//      * @param counts
//      * @param prPos
//      * @param prNeg
//      * @return The calculated Gain
//      */

    /*
     * Function to calculate the gain based on gini index
     */

    /* def gain(counts: Array[Int], prPos: Double, prNeg: Double): Double = {

      val Trsize: Double = counts(0) + counts(1)
      val Flsize: Double = counts(3) + counts(4)
      val Usize: Double = counts(2) + counts(5)// + counts(6) + counts(7)

      val size: Double =  Trsize + Flsize + Usize

      val startImpurity : Double = 	gini(counts(0) + counts(3), counts(1) + counts(4), prPos, prNeg)

      val TrImpurity = gini(counts(0), counts(1), prPos, prNeg)
      val FlImpurity = gini(counts(3), counts(4), prPos, prNeg)
      val UImpurity = gini(counts(2) , counts(5), prPos, prNeg) //counts(2)+ counts(6),  counts(5) + counts(7)

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
    } */



    /**
      * @param know knowledgebase
      * @param concept father concept
      * @param posExs RDD of positive examples
      * @param negExs RDD of negative examples
      * @param undExs RDD of undefined examples
      * @return
      */

    private def getSplitCounts(know: KB,
                               concept: OWLClassExpression,
                               posExs: RDD[String],
                               negExs: RDD[String],
                               undExs: RDD[String]): Array[Int] = {

      val counts: Array[Int] = Array.ofDim[Int](6)

      val Pos = splitGroup(know, concept, posExs)
      val Neg = splitGroup(know, concept, negExs)
      val Und = splitGroup(know, concept, undExs)

      counts(PL) = Pos._1.count.toInt
      counts(NL) = Neg._1.count.toInt
      counts(UL) = Und._1.count.toInt
      counts(PR) = Pos._2.count.toInt
      counts(NR) = Neg._2.count.toInt
      counts(UR) = Und._2.count.toInt

      counts
    }

    /**
      * @param know knowledgebase
      * @param concept Concept
      * @param nodeExamples RDD of node examples
      */
    private def splitGroup(know: KB,
                           concept: OWLClassExpression,
                           nodeExamples: RDD[String]): (RDD[String], RDD[String]) = {

      /* println("\nNode examples: \n ----------")
      nodeExamples.take(nodeExamples.count.toInt).foreach(println(_)) */

      val negConcept: OWLClassExpression = know.getDataFactory.getOWLObjectComplementOf(concept)

      var Left = new ArrayList[String]()
      var Right = new ArrayList[String]()

      for (e <- 0 until nodeExamples.count.toInt) {

        val nodeEx = nodeExamples.take(e + 1).apply(e)
        val nodeInd = know.getDataFactory.getOWLNamedIndividual(nodeEx).asInstanceOf[OWLIndividual]

        if (know.getReasoner.isEntailed(know.getDataFactory.getOWLClassAssertionAxiom(concept, nodeInd))) {
          Left.add(nodeEx)

        } else if (know.getReasoner.isEntailed(know.getDataFactory.getOWLClassAssertionAxiom(negConcept, nodeInd))) {
          Right.add(nodeEx)

        } else {
          Left.add(nodeEx)
          Right.add(nodeEx)
        }
      }

      val leftRDD = sc.sparkContext.parallelize(Left.asScala)
      val rightRDD = sc.sparkContext.parallelize(Right.asScala)

      /* println("\nleft ex: ")
      leftRDD.take(20).foreach(println(_))

      println("\nright ex: ")
      rightRDD.take(20).foreach(println(_)) */

      (leftRDD, rightRDD)


      // val propName: RDD[String] = know.getIndividuals().map( ind => ind.asOWLNamedIndividual().getIRI.getShortForm)
      //  println("\n nodeEx = " + nodeEx )
      // val Filtered = know.getIndividuals().filter(_ == nodeInd)
      // println("\n filtered = " )
      // Filtered.take(10).foreach(println(_))
      // val exIndex = ex.lookup(e)
      // println("the element: ")
      // exInd.take(1).foreach(println(_))
      // val ind  = know.getDataFactory().getOWLNamedIndividual(IRI.create(nodeEx)).asInstanceOf[OWLIndividual]
      // println("newexample  " + ind )

      // val x = know.getIndividuals().take(nodeExamples.count.toInt).apply(e)
      // val x = know.getIndividuals().filter( _ == neew).first()

      // x.take(20).foreach(println(_))

      // val r =know.getReasoner().isEntailed(know.getDataFactory.getOWLClassAssertionAxiom(concept, ind))
      // println("\n r = " + r)

      // val l =know.getReasoner().isEntailed(know.getDataFactory.getOWLClassAssertionAxiom(negConcept, ind))
      // println("\n l = " + l)
    }

    /**
      * @param know
      * @param concept
      * @param posExs
      * @param negExs
      * @param undExs
      */

    private def split(know: KB,
                      concept: OWLClassExpression,
                      posExs: RDD[String], negExs: RDD[String], undExs: RDD[String]):
    ((RDD[String], RDD[String]), (RDD[String], RDD[String]), (RDD[String], RDD[String])) = {

      val Pos = splitGroup(know, concept, posExs)
      val Neg = splitGroup(know, concept, negExs)
      val Und = splitGroup(know, concept, undExs)

      (Pos, Neg, Und)
    }

  } // class




//
//  /**
//    * Selecting the best in a list (RDD) of refinements using Entropy calculations
//    * @param know
//    * @param concepts
//    * @param posExs
//    * @param negExs
//    * @param undExs
//    * @param prPos
//    * @param prNeg
//    * @param truePosExs
//    * @param trueNegExs
//    * @return
//    */

  /* def selectBestConceptEntropy(know: KB, concepts: RDD[OWLClassExpression],
                                posExs: RDD[String],
                                negExs: RDD[String],
                                undExs: RDD[String],
                                prPos: Double, prNeg: Double,
                                truePosExs: RDD[String],
                                trueNegExs: RDD[String]): OWLClassExpression = {

     var bestConceptIndex: Int = 0
     var counts: Array[Int] = getSplitCounts(know, concepts.first(), posExs, negExs, undExs)

     println("%4s\t p:%d n:%d u:%d\t p:%d n:%d u:%d\t p:%d n:%d u:%d\t ", "#" + 0,
       counts(0), counts(1), counts(2), counts(3), counts(4), counts(5), counts(6), counts(7), counts(8))

     var minEntropy: Double = Entropy(counts, prPos, prNeg, truePosExs.count.toInt, trueNegExs.count.toInt)
     println("%+10e\n" + minEntropy)
     println(concepts.first())

     val n = concepts.zipWithIndex().map { case (x, y) => (y, x) }
     for (c <- 1 until concepts.count.toInt) {
       val nConcept = n.lookup(c).asInstanceOf[OWLClassExpression]
       counts = getSplitCounts(know, nConcept, posExs, negExs, undExs)
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
   } */

  /*
   * Function to calculate the Entropy value
   */

  /* def Entropy(counts: Array[Int], prPos: Double, prNeg: Double, sizPos: Int, sizNeg: Int): Double = {
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
   } */

} // object
