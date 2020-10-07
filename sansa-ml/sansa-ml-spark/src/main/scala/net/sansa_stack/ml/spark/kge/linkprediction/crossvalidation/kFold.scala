package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

import net.sansa_stack.rdf.spark.kge.triples.IntegerTriples
import org.apache.spark.sql._

/**
 * k-fold Cross Validation
 * -----------------------
 *
 * k-fold Cross Validation technique
 *
 * Created by lpfgarcia
 */

case class kException(info: String) extends Exception

case class withIndex(Subject: Int, Predicate: Int, Object: Int, k: Int)

class kFold(data: Dataset[IntegerTriples], k: Int, sk: SparkSession)
  extends CrossValidation[Seq[Dataset[IntegerTriples]]] {

  import sk.implicits._

  if (k > 1 && k <= 10) {
    throw new kException("The k value should be higher than 1 and lower or equal to 10")
  }

  val id = (1 to data.count().toInt / k).flatMap(List.fill(k)(_))
  val fold = sk.sparkContext.parallelize(id, data.rdd.getNumPartitions)

  def crossValidation(): (IndexedSeq[Dataset[IntegerTriples]], IndexedSeq[Dataset[IntegerTriples]]) = {

    val df = sk.createDataFrame(data.rdd.zip(fold).map { r =>
      withIndex(r._1.Subject, r._1.Predicate, r._1.Object, r._2)
    })

    val train = for (i <- 1 to k) yield {
      df.filter($"k" =!= i).drop("k").as[IntegerTriples]
    }

    val test = for (i <- 1 to k) yield {
      df.filter($"k" === i).drop("k").as[IntegerTriples]
    }

    (train, test)
  }

}
