package net.sansa_stack.ml.spark.kge.linkprediction.models

/**
 * DistMult: diagonal bilinear model
 * ---------------------------------
 *
 * Yang, Bishan, et al.
 * Learning multi-relational semantics using neural-embedding models." arXiv:1411.4072 (2014).
 *
 * Created by lpfgarcia on 20/11/2017.
 */

import org.apache.spark.sql._

import com.intel.analytics.bigdl.optim.Adam
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat

class DistMult(train: DataFrame, ne: Int, nr: Int, batch: Int, k: Int, sk: SparkSession)
    extends Models(ne: Int, nr: Int, batch: Int, k: Int, sk: SparkSession) {

  val epochs = 100
  val rate = 0.01f

  var opt = new Adam(learningRate = rate)

  def dist(data: DataFrame) = {
    val aux = data.collect().map { i =>
      e(i.getInt(0)) * r(i.getInt(1)) * e(i.getInt(2))
    }.reduce((a, b) => a + b)

    L2(aux)
  }

  def run() = {

    for (i <- 1 to epochs) {

      e = normalize(e)
      val pos = subset(train)
      val neg = negative(pos)

      def delta(x: Tensor[Float]) = {
        (dist(neg) - dist(pos) + 1, x)
      }

      opt.optimize(delta, e)
      opt.optimize(delta, r)
      val err = dist(pos) - dist(neg) + 1
      printf("Epoch: %d: %f\n", i, err)

    }
  }

}