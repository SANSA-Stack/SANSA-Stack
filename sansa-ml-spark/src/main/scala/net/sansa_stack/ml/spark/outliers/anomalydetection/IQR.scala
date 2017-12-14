package net.sansa_stack.ml.spark.outliers.anomalydetection

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Node
object IQR {
  
  def iqr(sampleData: List[Double], cluster: RDD[Set[(Node, Node, Object)]]) {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("summary example")
      .getOrCreate()

    //create sample data 

    val c = sampleData.sorted

    val rowRDD = sparkSession.sparkContext.makeRDD(c.map(value => Row(value)))
    val schema = StructType(Array(StructField("value", DoubleType)))
    val df = sparkSession.createDataFrame(rowRDD, schema)
    df.show
    // calculate quantiles and IQR
    val quantiles = df.stat.approxQuantile("value",
      Array(0.25, 0.75), 0.0)
    //quantiles.foreach(println)

    val Q1 = quantiles(0)

    val Q3 = quantiles(1)

    val IQR = Q3 - Q1

    val lowerRange = Q1 - 1.5 * IQR

    val upperRange = Q3 + 1.5 * IQR

    val outliers = df.filter(s"value < $lowerRange or value > $upperRange")
    
    println("showing outlier")
    outliers.show


  }

}
