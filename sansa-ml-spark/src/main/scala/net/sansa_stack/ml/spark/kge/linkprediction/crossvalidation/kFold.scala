package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

/**
 * Created by lpfgarcia on 24/11/2017.
 */

import org.apache.spark.sql._
import org.apache.spark.sql.types.IntegerType

case class kException(info: String) extends Exception

class kFold(data: DataFrame, k: Int, sk: SparkSession) extends CrossValidation[Seq[DataFrame]] {

  import sk.implicits._

  if (k > 1 && k <= 10)
    throw new kException("The k value should be higher than 1 and lower or equal to 10")

  val id = (1 to data.count().toInt / k).flatMap(List.fill(k)(_))
  val fold = sk.sparkContext.parallelize(id, data.rdd.getNumPartitions)

  def crossValidation() = {

    val tmp = data.rdd.zip(fold).map(r => Row.fromSeq(r._1.toSeq ++ Seq(r._2)))
    val df = sk.createDataFrame(tmp, data.schema.add("k", IntegerType))

    val train = for (i <- 1 to k) yield {
      df.filter($"k" =!= i).drop("k")
    }

    val test = for (i <- 1 to k) yield {
      df.filter($"k" === i).drop("k")
    }

    (train, test)
  }

}