package net.sansa_stack.ml.spark.mining.amieSpark

import java.io.File
import java.net.URI

import scala.collection.mutable.{ ArrayBuffer, Map }
import scala.util.Try

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SparkSession, SQLContext, _ }

import net.sansa_stack.ml.spark.mining.amieSpark.DfLoader.Atom
import net.sansa_stack.ml.spark.mining.amieSpark.KBObject.KB
import net.sansa_stack.ml.spark.mining.amieSpark.Rules.RuleContainer


object MineRules {
  /**
   * Algorithm that mines the Rules.
   *
   * @param kb object knowledge base that was created in main
   * @param minHC threshold on head coverage
   * @param maxLen maximal rule length
   * @param threshold on confidence
   * @param sc spark context
   *
   *
   */
  class Algorithm(k: KB, mHC: Double, mL: Int, mCon: Double, hdfsP: String) extends Serializable {

    val kb: KB = k
    val minHC = mHC
    val maxLen = mL
    val minConf = mCon
    val hdfsPath = hdfsP

    def calcName(whole: ArrayBuffer[RDFTriple]): String = {

      var countMap: Map[String, Int] = Map()
      var numberMap: Map[String, Int] = Map()
      var counter: Int = 1
      for (w <- whole) {
        if (countMap.contains(w._1)) {
          var temp = countMap.remove(w._1).get + 1
          countMap += (w._1 -> temp)
        } else {
          countMap += (w._1 -> 1)
        }
        if (countMap.contains(w._3)) {
          var temp = countMap.remove(w._3).get + 1
          countMap += (w._3 -> temp)
        } else {
          countMap += (w._3 -> 1)
        }
        if (!numberMap.contains(w._1)) {
          numberMap += (w._1 -> counter)
          counter += 1
        }
        if (!numberMap.contains(w._3)) {
          numberMap += (w._3 -> counter)
          counter += 1
        }

      }

      var out = ""
      for (wh <- whole) {
        var a = ""
        var b = ""
        if (countMap(wh._1) > 1) {
          a = numberMap(wh._1).toString
        } else {
          a = "0"
        }

        if (countMap(wh._3) > 1) {
          b = numberMap(wh._3).toString
        } else {
          b = "0"
        }

        out += a + "_" + wh._2 + "_" + b + "_"
      }
      out = out.stripSuffix("_")
      out
    }

    def ruleMining(sc: SparkContext, sqlContext: SQLContext): ArrayBuffer[RuleContainer] = {

      var predicates = kb.getKbGraph.triples.map { x => x.predicate

      }.distinct
      var z = predicates.collect()
      println(s"#predicates:$z.length")

      /**
       * q is a queue with one atom rules
       * is initialized with every distinct relation as head, body is empty
       */
      var q = ArrayBuffer[RuleContainer]()

      for (zz <- z) {
        if (zz != null) {
          var rule = ArrayBuffer(RDFTriple("?a", zz, "?b"))

          var rc = new RuleContainer
          rc.initRule(rule, kb, sc, sqlContext)

          q += rc
        }

      }

      var outMap: Map[String, ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]] = Map()

      var dataFrameRuleParts: RDD[(RDFTriple, Int, Int)] = null
      var out: ArrayBuffer[RuleContainer] = new ArrayBuffer
      var dublicate: ArrayBuffer[String] = ArrayBuffer("")

      for (i <- 0 until this.maxLen) {

        if ((i > 0) && (dataFrameRuleParts != null)) {
          var temp = q.clone

          q = new ArrayBuffer

          var newAtoms1 = dataFrameRuleParts.collect

          for (n1 <- newAtoms1) {

            var newRuleC = new RuleContainer
            var parent = temp(n1._3)
            var newTpArr = parent.getRule().clone

            newTpArr += n1._1
            var fstTp = newTpArr(0).toString()
            var counter = 1
            var sortedNewTpArr = new ArrayBuffer[RDFTriple]

            if (newTpArr.length > 2) {
              sortedNewTpArr = sort(newTpArr.clone)
            } else {
              sortedNewTpArr = newTpArr.clone
            }

            var dubCheck = fstTp

            for (i <- 1 until newTpArr.length) {
              var temp = newTpArr(i).toString
              dubCheck += sortedNewTpArr(i).toString
              if (temp == fstTp) {
                counter += 1
              }
            }
            if ((counter < newTpArr.length) && (!dublicate.contains(dubCheck))) {
              dublicate += dubCheck
              newRuleC.setRule(minConf, n1._2, parent, newTpArr, sortedNewTpArr, kb, sc, sqlContext)
              q += newRuleC
            }

          }

        } else if ((i > 0) && ((dataFrameRuleParts == null) || dataFrameRuleParts.isEmpty())) {
          q = new ArrayBuffer
        }

        if (q.nonEmpty) {
          for (j <- q.indices) {

            val r: RuleContainer = q(j)

            var tp = r.getRule()
            if (tp.length > 2) {
              tp = r.getSortedRule()

            }

            if (acceptedForOutput(outMap, r, minConf, kb, sc, sqlContext)) {
              out += r

              if (!outMap.contains(tp(0).predicate)) {
                outMap += (tp(0).predicate -> ArrayBuffer((tp, r)))
              } else {
                var temp: ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)] = outMap.remove(tp(0).predicate).get
                temp += Tuple2(tp, r)
                outMap += (tp(0).predicate -> temp)

              }

            }
            var R = new ArrayBuffer[RuleContainer]()

            if (r.getRule().length < maxLen) {

              dataFrameRuleParts = refine(i, j, r, dataFrameRuleParts, sc, sqlContext)
              // TODO: Dublicate check

            }

          }
        }

      }

      out
    }

    /**
     * exploring the search space by iteratively extending rules using a set of mining operators:
     * - add dangling atom
     * - add instantiated atom
     * - add closing atom
     *
     */

    def refine(c: Int, id: Int, r: RuleContainer, dataFrameRuleParts: RDD[(RDFTriple, Int, Int)], sc: SparkContext, sqlContext: SQLContext): RDD[(RDFTriple, Int, Int)] = {

      var out: DataFrame = null
      var OUT: RDD[(RDFTriple, Int, Int)] = dataFrameRuleParts
      // var count2:RDD[(String, Int)] = null
      var path = new File("test_table/")
      var temp = 0

      val tpAr = r.getRule()

      var stringSELECT = ""
      for (tp <- tpAr.indices) {

        stringSELECT += "tp" + tp + ", "

      }

      stringSELECT += "tp" + tpAr.length

      var z: Try[Row] = null
      if ((tpAr.length != maxLen - 1) && (temp == 0)) {
        var a = kb.addDanglingAtom(c, id, minHC, r, sc, sqlContext)

        z = Try(a.first())
        if ((!z.isFailure) && z.isSuccess) {

          out = a

        }

      }

      var b = kb.addClosingAtom(c, id, minHC, r, sc, sqlContext)

      var t = Try(b.first)

      if ((!t.isFailure) && t.isSuccess && (temp == 0)) {

        if (out == null) {
          out = b
        } else {
          out = out.unionAll(b)

        }

      }

      var count: RDD[(String, Int)] = null
      var o: RDD[(RDFTriple, Int, Int)] = null

      if (((!t.isFailure) && t.isSuccess) || ((z != null) && (!z.isFailure) && z.isSuccess)) {
        count = out.rdd.map(x => (x(r.getRule().length + 1).toString, 1)).reduceByKey(_ + _)

        o = count.map(q => (q._1.split("\\s+"), q._2)).map { token =>
          Tuple3(RDFTriple(token._1(0), token._1(1), token._1(2)), token._2, token._1(3).toInt)
        }.filter(n1 => n1._2 >= (kb.getRngSize(n1._1.predicate) * minHC))

        if (OUT == null) {
          OUT = o
        } else {
          OUT = OUT.union(o)
        }

      }

      OUT

    }

    /**
     * checks if rule is a useful output
     *
     * @param out output
     * @param r rule
     * @param minConf min. confidence
     *
     */
    def acceptedForOutput(outMap: Map[String, ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]], r: RuleContainer, minConf: Double, k: KB, sc: SparkContext, sqlContext: SQLContext): Boolean = {

      // if ((!(r.closed())) || (r.getPcaConfidence(k, sc, sqlContext) < minConf)) {
      if ((!r.closed()) || (r.getPcaConfidence() < minConf)) {
        return false

      }

      var parents: ArrayBuffer[RuleContainer] = r.parentsOfRule(outMap, sc)
      if (r.getRule().length > 2) {
        for (rp <- parents) {
          if (r.getPcaConfidence() <= rp.getPcaConfidence()) {
            return false
          }

        }
      }

      true
    }

    def sort(tp: ArrayBuffer[RDFTriple]): ArrayBuffer[RDFTriple] = {
      var out = ArrayBuffer(tp(0))
      var temp = new ArrayBuffer[(String, RDFTriple)]

      for (i <- 1 until tp.length) {
        var tempString: String = tp(i).predicate + tp(i).subject + tp(i).`object`
        temp += Tuple2(tempString, tp(i))

      }
      temp = temp.sortBy(_._1)
      for (t <- temp) {
        out += t._2
      }

      out
    }

  }
}
