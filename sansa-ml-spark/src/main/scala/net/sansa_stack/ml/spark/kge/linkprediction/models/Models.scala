package net.sansa_stack.ml.spark.kge.linkprediction.models

/**
 * Model Abstract Class
 * --------------------
 *
 * Created by lpfgarcia on 14/11/2017.
 */

import scala.math._
import scala.util._

import org.apache.spark.sql._

import com.intel.analytics.bigdl.nn.Power
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat

abstract class Models(ne: Int, nr: Int, batch: Int, k: Int, sk: SparkSession) {

  val Ne = ne
  val Nr = nr

  var e = initialize(ne)
  var r = normalize(initialize(nr))

  def initialize(size: Int) = {
    Tensor(size, k).rand(-6 / sqrt(k), 6 / sqrt(k))
  }

  def normalize(data: Tensor[Float]) = {
    data / data.abs().sum()
  }

  import sk.implicits._

  val seed = new Random(System.currentTimeMillis())

  def tuple(aux: Row) = {
    if (seed.nextBoolean()) {
      (seed.nextInt(Ne) + 1, aux.getInt(1), aux.getInt(2))
    } else {
      (aux.getInt(0), aux.getInt(1), seed.nextInt(Ne) + 1)
    }
  }

  def negative(data: DataFrame) = {
    data.collect().map(i =>
      tuple(i)).toSeq.toDF()
  }

  def subset(data: DataFrame) = {
    data.sample(false, 2 * (batch.toDouble / data.count().toDouble)).limit(batch).toDF()
  }

  def L1(vec: Tensor[Float]) = {
    vec.abs().sum()
  }

  def L2(vec: Tensor[Float]) = {
    vec.pow(2).sqrt().sum()
  }

}