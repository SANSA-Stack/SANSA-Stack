package net.sansa_stack.ml.spark.classification.decisionTrees.Utils

import org.apache.commons.math3.stat.StatUtils

object StatsUtils {
  
  
  private var mean = .0
  private var stddev = .0
  
  def average(rate: Array[Double]): Double = {
    mean = StatUtils.mean(rate)
    mean
  }
  
  def variance(arr: Array[Double]): Double = {
    stddev = StatUtils.variance(arr)
    stddev
  }
  
  
  def stdDeviation(arr: Array[Double]): Double = {
    stddev = StatUtils.variance(arr)
    Math.sqrt(stddev)
  
  }
}
