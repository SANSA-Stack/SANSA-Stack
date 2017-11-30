package net.sansa_stack.ml.spark.kge.linkprediction.models

import scala.math._
import scala.util._

import org.apache.spark.sql._

import com.intel.analytics.bigdl.nn.Power
import com.intel.analytics.bigdl.optim.Adam
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat

import net.sansa_stack.ml.spark.kge.linkprediction.dataset.Dataset

class TransE(train: Dataset, m: Float, k: Int, L: String, sk: SparkSession) extends Models {

  val batch = 100
  val epochs = 100
  val rate = 0.01f

  var e = initialize(train.s)
  var l = normalize(initialize(train.p))

  val myL = L match {
    case "L2" => L2 _
    case _    => L1 _
  }

  def initialize(data: Array[String]) = {
    Tensor(data.length, k).rand(-6 / sqrt(k), 6 / sqrt(k))
  }

  def normalize(data: Tensor[Float]) = {
    for (i <- 1 to k)
      data(i) / data(i).abs().sum()
    data
  }

  import sk.implicits._

  def subset(data: DataFrame) = {
    data.sample(false, (batch / data.count()).toDouble).toDF()
  }

  val seed = new Random(System.currentTimeMillis())

  def tuple(aux: Row, idx: Boolean) = {

    val rd = seed.nextInt(train.s.length) + 1

    if (idx) {
      (rd, aux.getInt(1), aux.getInt(2))
    } else {
      (aux.getInt(0), aux.getInt(1), rd)
    }
  }

  def generate(data: DataFrame) = {

    val rnd = seed.nextBoolean()
    var aux = Seq[(Int, Int, Int)]()

    for (j <- 1 to data.count().toInt) {
      val i = data.rdd.take(j).last
      aux = tuple(i, rnd) +: aux
    }

    aux.toDF()
  }

  def L1(vec: Row) = {
    (e(vec.getInt(0)) + l(vec.getInt(1)) - e(vec.getInt(2))).abs().sum().toFloat
  }

  def L2(vec: Row) = {
    val p = Power(2, 1, 1)
    (p.forward(e(vec.getInt(0)) + l(vec.getInt(1)) - e(vec.getInt(2)))).sqrt().sum().toFloat
  }

  def run() = {

    var ept = new Adam(learningRate = rate)
    var lpt = new Adam(learningRate = rate)

    for (i <- 1 to epochs) {

      e = normalize(e)
      val pos = subset(train.df)
      val neg = generate(pos)
      var err: Float = 0

      for (j <- 1 to pos.count().toInt) {

        val ep = pos.rdd.take(j).last
        val en = neg.rdd.take(j).last

        def delta(x: Tensor[Float]) = {
          (myL(ep) - myL(en), x)
        }

        if (m + myL(ep) > myL(en)) {

          ept.optimize(delta, e)
          lpt.optimize(delta, l)
          err += m + myL(ep) - myL(en)
        }
      }

      printf("Epoch: %d: %f\n", i, err)
    }
  }

}