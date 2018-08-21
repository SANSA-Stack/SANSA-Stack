package net.sansa_stack.ml.spark.kge.linkprediction.models

import com.intel.analytics.bigdl.optim.Adam
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import net.sansa_stack.rdf.spark.kge.triples.{ IntegerTriples, StringTriples }
import org.apache.spark.sql._

/**
 * DistMult: diagonal bilinear model
 * ---------------------------------
 *
 * Yang, Bishan, et al.
 * Learning multi-relational semantics using neural-embedding models." arXiv:1411.4072 (2014).
 *
 * Created by lpfgarcia on 20/11/2017.
 */
class DistMult(train: Dataset[IntegerTriples], ne: Int, nr: Int, batch: Int, k: Int, sk: SparkSession)
  extends Models(ne: Int, nr: Int, batch: Int, k: Int, sk: SparkSession) {

  val epochs = 100
  val rate = 0.01f

  var opt = new Adam(learningRate = rate)

  def dist(data: Dataset[IntegerTriples]): Float = {
    val aux = data.collect().map { i =>
      e(i.Subject) * r(i.Predicate) * e(i.Object)
    }.reduce((a, b) => a + b)

    L2(aux)
  }

  def run(): Unit = {

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
