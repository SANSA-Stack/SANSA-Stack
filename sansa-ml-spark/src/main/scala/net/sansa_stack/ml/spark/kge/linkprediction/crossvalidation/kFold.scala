package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

/**
 * k-fold Cross Validation
 * -----------------------
 *
 * Created by lpfgarcia on 24/11/2017.
 */

import org.apache.spark.sql._
import org.apache.spark.sql.types.IntegerType

import net.sansa_stack.ml.spark.kge.linkprediction.dataframe._

case class kException(info: String) extends Exception

case class withIndex(Subject: Int, Predicate: Int, Object: Int, k: Int)

class kFold(data: Dataset[IntegerRecord], k: Int, sk: SparkSession) extends CrossValidation[Seq[Dataset[IntegerRecord]]] {

  import sk.implicits._

  if (k > 1 && k <= 10)
    throw new kException("The k value should be higher than 1 and lower or equal to 10")

  val id = (1 to data.count().toInt / k).flatMap(List.fill(k)(_))
  val fold = sk.sparkContext.parallelize(id, data.rdd.getNumPartitions)

  def crossValidation() = {

    val df = data.toDF().rdd.zip(fold).map { r =>
      Seq(r._1(0), r._1(1), r._1(2), r._2)
    }.toDF()

    val train = for (i <- 1 to k) yield {
      df.filter($"k" =!= i).drop("k").map { i =>
        IntegerRecord(i.getInt(0), i.getInt(1), i.getInt(2))
      }
    }

    val test = for (i <- 1 to k) yield {
      df.filter($"k" === i).drop("k").map { i =>
        IntegerRecord(i.getInt(0), i.getInt(1), i.getInt(2))
      }
    }

    (train, test)
  }

}