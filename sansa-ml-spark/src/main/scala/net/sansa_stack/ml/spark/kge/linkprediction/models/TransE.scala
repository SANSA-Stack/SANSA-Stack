package net.sansa_stack.ml.spark.kge.linkprediction.models

/**
 * TransE embedding model
 * ----------------------
 *
 * Bordes, Antoine, et al.
 * Translating embeddings for modeling multi-relational data. NIPS. 2013.
 *
 * Created by lpfgarcia on 14/11/2017.
 */

import scala.math._

import org.apache.spark.sql._

import com.intel.analytics.bigdl.optim.Adam
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat

import net.sansa_stack.rdf.spark.kge.triples.{StringTriples,IntegerTriples}

class TransE(train: Dataset[IntegerTriples], ne: Int, nr: Int, batch: Int, k: Int, margin: Float, L: String, sk: SparkSession)
    extends Models(ne: Int, nr: Int, batch: Int, k: Int, sk: SparkSession) {

  val epochs = 1000
  val rate = 0.01f

  var opt = new Adam(learningRate = rate)

  val myL = L match {
    case "L2" => L2 _
    case _    => L1 _
  }

  import sk.implicits._

  def dist(data: Dataset[IntegerTriples]) = {

    val aux = data.collect().map { i =>
      e(i.Subject) + r(i.Predicate) - e(i.Object)
    }.reduce((a, b) => a + b)

    myL(aux)
  }

  def dist(row: IntegerTriples) = {
    e(row.Subject) + r(row.Predicate) - e(row.Object)
  }

  def run() = {

    for (i <- 1 to epochs) {

      e = normalize(e)
      val pos = subset(train)
      val neg = negative(pos)

      def delta(x: Tensor[Float]) = {
        (signum(margin * batch + dist(pos) - dist(neg)), x)
      }

      if (margin * batch + dist(pos) > dist(neg)) {

        opt.optimize(delta, e)
        opt.optimize(delta, r)
        val err = margin * batch + dist(pos) - dist(neg)
        printf("Epoch: %d: %f\n", i, err)
      }

    }
  }

}