package net.sansa_stack.ml.spark.outliers.vandalismdetection



import org.apache.spark.{ SparkConf, SparkContext }



object Main {
  
  
  def main(args:Array[String]){
    

  val start = new VandalismDetection()
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("VandalismDetector")
  val sc = new SparkContext(sparkConf)

  start.Triger(sc)

  
  
  
  }
}