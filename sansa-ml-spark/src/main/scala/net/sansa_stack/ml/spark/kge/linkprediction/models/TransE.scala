package net.sansa_stack.ml.spark.kge.linkprediction.models

/**
 * Created by lpfgarcia on 14/11/2017.
 */

import scala.math._

import org.apache.spark.sql._

import com.intel.analytics.bigdl.optim.Adam
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat

class TransE(train: DataFrame, batch: Int, k: Int, margin: Float, L: String, sk: SparkSession)
    extends Models(train: DataFrame, batch: Int, k: Int, sk: SparkSession) {

  val epochs = 1000
  val rate = 0.01f

  var opt = new Adam(learningRate = rate)

  val myL = L match {
    case "L2" => L2 _
    case _    => L1 _
  }

  def dist(data: DataFrame) = {
    val aux = data.collect().map { i =>
      e(i.getInt(0)) + r(i.getInt(1)) - e(i.getInt(2))
    }.reduce((a, b) => a + b)

    myL(aux)
  }

  def run() = {

    for (i <- 1 to epochs) {

      e = normalize(e)
      val pos = subset(train)
      val neg = generate(pos)

      def delta(x: Tensor[Float]) = {
        (signum(margin + dist(pos) - dist(neg)), x)
      }

      if (margin * batch + dist(pos) > dist(neg)) {

        opt.optimize(delta, e)
        val err = margin + dist(pos) - dist(neg)
        printf("Epoch: %d: %f\n", i, err)
      }

    }
  }

}