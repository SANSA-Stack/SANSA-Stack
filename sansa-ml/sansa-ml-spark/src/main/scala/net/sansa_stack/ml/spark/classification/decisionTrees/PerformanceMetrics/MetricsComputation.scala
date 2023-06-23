package net.sansa_stack.ml.spark.classification.decisionTrees.PerformanceMetrics

import net.sansa_stack.ml.spark.classification.decisionTrees.Utils
import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model._

  /**
   * This class for calculating the performance of a classifier, which supports the following performance
   * metrics: match, commission, omission, induction rates.
   *
   * Performance metrics names refer to the names used in
   * 'Induction of concepts in web ontologies through terminological decision trees'
   * by Fanizzi Nicola, Claudia d’Amato, and Floriana Esposito
   *
   * @author Heba Mohamed
   */
 
  class MetricsComputation (noConcepts: Int, nofolds: Int) extends Serializable {
  

    
    var results: RDD[(Int, Int, Int, Int, Int, Int, Int)] = _
    
    // matching, omission, commission and induction rates per OWLClass per fold
     /** Matching rate: the number of individuals that received the same classification
      *  using reasoner and TDT
     */
    var matchingRate: Array[Array[Double]] = Array.ofDim[Double](noConcepts, nofolds)
  
    /** Omission error rate: The number of individuals whose membership in relation to the provided query
     * could not be identified using TDT, despite the fact that they may be proved to belong to the
     * query concept or its complement.
     */
    var omissionRate: Array[Array[Double]] = Array.ofDim[Double](noConcepts, nofolds)
  
    /** Commission error rate: According to the induced TDT, the number of individuals detected belong
     * to the query concept, while they may be proven to belong to its complement, and vice-versa.
     */
    var commissionRate: Array[Array[Double]] = Array.ofDim[Double](noConcepts, nofolds)
  
    /** Induction rate: The number of individuals detected to belong to the query concept
     * or its complement according to the TDT, whereas neither instance can be derived
     * logically from the knowledge base.
     */
    var inductionRate: Array[Array[Double]] = Array.ofDim[Double](noConcepts, nofolds)
    
    var recallRate: Array[Array[Double]] = Array.ofDim[Double](noConcepts, nofolds)
    
    var precisionRate: Array[Array[Double]] = Array.ofDim[Double](noConcepts, nofolds)
    
    
    
    /**
     * Classifies all the test individuals for the current fold w.r.t. the ground truth for the c-th concept
     *
     * @param f                 , current fold number
     * @param labels            , rdd of the predictions
     * @param classification    , rdd of the ground truth
     * @param testExamples      , rdd of the test examples
     */
    def computeMetricsPerFold(fold: Int,
                              labels: RDD[((OWLClassExpression, OWLIndividual), Int)],
                              classification: RDD[((OWLClassExpression, OWLIndividual), Int)],
                              testExamples: RDD[OWLIndividual]): Unit = {

      
      val joinedRDD: RDD[((OWLClassExpression, OWLIndividual), (Int, Int))] = labels.join(classification)
      val size = testExamples.count()
//      joinedRDD.foreach(println(_))
      var matchingNum = 0
      var commissionNum = 0
      var omissionNum = 0
      var inducedNum = 0
      var foundNum = 0
      var trueNum = 0
      var hitNum = 0
      
      for (c <- 0 until noConcepts) {

        results = joinedRDD
            .mapPartitions(partition => partition
            .map { j =>
                  if (j._2._1 == 1) foundNum += 1
                  if (j._2._2 == +1) trueNum += 1
            
                  // match rate handles the following cases: (0,0), (1,1) and (-1,-1) for the joindRDD
                  if (j._2._1 == j._2._2) {
                    matchingNum += 1
                    if (j._2._2 == 1) hitNum += 1
                  } // commission error rate handles the following cases: (1,-1) and (-1,1) for the joindRDD
                  else if ((j._2._1 - j._2._2).abs > 1) {
                    commissionNum += 1
                  } // omission error rate handles the following cases: (0,-1) and (0,1) for the joindRDD
                  else if (j._2._2 != 0) {
                    omissionNum += 1
                  } // induction rate handles the following cases: (1,0) and (-1,0) for the joindRDD
                  else {
                    inducedNum += 1
                  }
                  (matchingNum, commissionNum, omissionNum, inducedNum, foundNum, trueNum, hitNum)
                }
               ).coalesce(1)
      //        results.foreach(println(_))
      }
      
      matchingNum = results.map(r => r._1).max()
      commissionNum = results.map(r => r._2).max()
      omissionNum = results.map(r => r._3).max()
      inducedNum = results.map(r => r._4).max()
      foundNum = results.map(r => r._5).max()
      trueNum = results.map(r => r._6).max()
      hitNum = results.map(r => r._7).max()
      
      println("\n\n ++++++++++++++++++++++++++++++++++ FOLD #" + fold + " RESULTS +++++++++++++++++++++++++++++++++++++++++")
      println("Query#" + "\tmatching" + "\t\tcommission" + "\t\tomission" + "\t\tinduction" + "\t\tprecision" + "\t\trecall")
      
      for (c <- 0 until noConcepts) {
        matchingRate(c)(fold-1) = matchingNum / size.toDouble
        commissionRate(c)(fold-1) = commissionNum / size.toDouble
        omissionRate(c)(fold-1) = omissionNum / size.toDouble
        inductionRate(c)(fold-1) = inducedNum / size.toDouble
        precisionRate(c)(fold-1) = (hitNum + 1).toDouble / (foundNum + 1).toDouble
        recallRate(c)(fold-1) = (hitNum + 1).toDouble / (trueNum + 1).toDouble
        
        val m = matchingRate(c)(fold-1)
        val co = commissionRate(c)(fold-1)
        val o = omissionRate(c)(fold-1)
        val i = inductionRate(c)(fold-1)
        val p = precisionRate(c)(fold-1)
        val r = recallRate(c)(fold-1)

        println((c + 1) + "\t    " + f"$m%8.2f " + "\t    " + f"$co%8.2f " +
                    "\t   " + f"  $o%8.2f " + "\t    " + f"$i%8.2f" + "\t    "
                    + f"$p%8.2f" + "\t    " + f"$r%8.2f")
      }
    }
  

    /**
     * Average the performance over the number of target concepts
     *
     * @param noConcepts , number of query concepts
     */
    def computeOverAllResults(noConcepts: Int): Unit = {
  
      // Every rate is per query concept per fold
      val matchingAvgArray = new Array[Double](noConcepts)
      val commissionAvgArray = new Array[Double](noConcepts)
      val omissionAvgArray = new Array[Double](noConcepts)
      val inducedAvgArray = new Array[Double](noConcepts)
      val precisionAvgArray = new Array[Double](noConcepts)
      val recallAvgArray = new Array[Double](noConcepts)
      
  
      println("\n\n ++++++++++++++++++++++++++++++++++++++ OVERALL RESULTS ++++++++++++++++++++++++++++++++++++++++++")
      println("Query#" + "\tmatching" + "\t\tcommission" + "\t\tomission" + "\t\tinduction" + "\t\tprecision" + "\t\trecall")
  
  
      for (c <- 0 until noConcepts) {
        
        val AvgMatching = Utils.StatsUtils.average(matchingRate(c))
        matchingAvgArray(c) = AvgMatching
       
        val AvgCommission = Utils.StatsUtils.average(commissionRate(c))
        commissionAvgArray(c) = AvgCommission
       
        val avgOmission = Utils.StatsUtils.average(omissionRate(c))
        omissionAvgArray(c) = avgOmission
       
        val avgInduction = Utils.StatsUtils.average(inductionRate(c))
        inducedAvgArray(c) = avgInduction
  
        val avgPrecision = Utils.StatsUtils.average(precisionRate(c))
        precisionAvgArray(c) = avgPrecision
  
        val avgRecall = Utils.StatsUtils.average(recallRate(c))
        recallAvgArray(c) = avgRecall
  
        println((c + 1) + "\t    " + f"$AvgMatching%8.2f" + "\t    " + f"$AvgCommission%8.2f" +
                    "\t    " + f"$avgOmission%8.2f" + "\t    " + f"$avgInduction%8.2f" +
                    "\t    " + f"$avgPrecision%8.2f" + "\t    " + f"$avgRecall%8.2f")
      }
 
      println("\n*************************************************************************")

      val matchingAvg = Utils.StatsUtils.average(matchingAvgArray) * 100
      val matchingSD = Utils.StatsUtils.stdDeviation(matchingAvgArray) * 100
      
      val commissionAvg = Utils.StatsUtils.average(commissionAvgArray) * 100
      val commissionSD = Utils.StatsUtils.stdDeviation(commissionAvgArray) * 100
      
      val omissionAvg = Utils.StatsUtils.average(omissionAvgArray) * 100
      val omissionSD = Utils.StatsUtils.stdDeviation(omissionAvgArray) * 100
      
      val inductionAvg = Utils.StatsUtils.average(inducedAvgArray) * 100
      val inductionSD = Utils.StatsUtils.stdDeviation(inducedAvgArray) * 100
  
      val precisionAvg = Utils.StatsUtils.average(precisionAvgArray) * 100
      val precisionSD = Utils.StatsUtils.stdDeviation(precisionAvgArray) * 100
  
      val recallAvg = Utils.StatsUtils.average(recallAvgArray) * 100
      val recallSD = Utils.StatsUtils.stdDeviation(recallAvgArray) * 100
  
      println("avg Values   " + "\t" + f"$matchingAvg%8.2f " + "\t" + f"$commissionAvg%8.2f " +
              "\t" + f"$omissionAvg%8.2f " + "\t" + f"$inductionAvg%8.2f " +
              "\t" + f"$precisionAvg%8.2f " + "\t" + f"$recallAvg%8.2f ")
  
      println("stdDev Values" + "\t" + f"$matchingSD%8.2f " + "\t" + f"$commissionSD%8.2f " +
        "\t" + f"$omissionSD%8.2f " + "\t" + f"$inductionSD%8.2f " +
        "\t" + f"$precisionSD%8.2f " + "\t" + f"$recallSD%8.2f ")
     }
  }
