package net.sansa_stack.ml.spark.mining.amieSpark

import java.io.File
import java.net.URI

import net.sansa_stack.ml.spark.mining.amieSpark.KBObject.KB
import net.sansa_stack.ml.spark.mining.amieSpark.Rules.RuleContainer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext, SparkSession, _ }

import scala.collection.mutable.{ ArrayBuffer, Map }
import scala.util.Try

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import net.sansa_stack.rdf.spark.model.JenaSparkRDDOps
import net.sansa_stack.rdf.spark.model.TripleRDD._
import net.sansa_stack.ml.spark.mining.amieSpark.DfLoader.Atom

object MineRules {
  /**
   * 	Algorithm that mines the Rules.
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

    def deleteRecursive(path: File): Int = {

      var files = path.listFiles()
      if (files != null) {
        for (f <- files) {
          if (f.isDirectory()) {

            deleteRecursive(f)
            f.delete()
          } else {

            f.delete()

          }
        }

        path.delete()

      }

      return 0

    }

    def ruleMining(sc: SparkContext, sqlContext: SQLContext): ArrayBuffer[RuleContainer] = {

      val fs: FileSystem = FileSystem.get(new URI("hdfs://akswnc5.informatik.uni-leipzig.de:54310/Theresa/"), sc.hadoopConfiguration);
      fs.delete(new Path("/Theresa/permanent0/"), true)
      fs.delete(new Path("/Theresa/permanent1/"), true)

      var predicates = kb.getKbGraph().triples.map { x => x.predicate

      }.distinct
      var z = predicates.collect()

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

      for (i <- 0 to this.maxLen - 1) {

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

            for (i <- 1 to newTpArr.length - 1) {
              var temp = newTpArr(i).toString
              dubCheck += sortedNewTpArr(i).toString
              if (temp == fstTp) {
                counter += 1
              }
            }
            if ((counter < newTpArr.length) && (!(dublicate.contains(dubCheck)))) {
              dublicate += dubCheck
              newRuleC.setRule(minConf, n1._2, parent, newTpArr, sortedNewTpArr, kb, sc, sqlContext)
              q += newRuleC
            }

          }

        } else if ((i > 0) && (dataFrameRuleParts.isEmpty())) {
          q = new ArrayBuffer
        }

        if ((!q.isEmpty)) {
          for (j <- 0 to q.length - 1) {

            val r: RuleContainer = q(j)

            var tp = r.getRule()
            if (tp.length > 2) {
              tp = r.getSortedRule()

            }

            if (acceptedForOutput(outMap, r, minConf, kb, sc, sqlContext)) {
              out += r

              if (!(outMap.contains(tp(0).predicate))) {
                outMap += (tp(0).predicate -> ArrayBuffer((tp, r)))
              } else {
                var temp: ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)] = outMap.remove(tp(0).predicate).get
                temp += new Tuple2(tp, r)
                outMap += (tp(0).predicate -> temp)

              }

            }
            var R = new ArrayBuffer[RuleContainer]()

            if (r.getRule().length < maxLen) {

              dataFrameRuleParts = refine(i, j, r, dataFrameRuleParts, sc, sqlContext)
              //TODO: Dublicate check

            }

          }
        }

      }

      return out
    }

    /**
     * checks if rule is a useful output
     *
     * @param out output
     * @param r rule
     * @param minConf min. confidence
     *
     */

    /**
     * exploring the search space by iteratively extending rules using a set of mining operators:
     * - add dangling atom
     * - add instantiated atom
     * - add closing atom
     *
     */

    def parquetToDF(path: File, sqlContext: SQLContext): DataFrame = {
      var x: DataFrame = null

      var tester = path.listFiles()
      if (tester != null) {
        for (te <- tester) {
          var part = sqlContext.read.parquet(te.toString)
          if (x == null) {
            x = part
          } else {
            x = x.union(part)
          }
        }
      }
      return x
    }

    def refine(c: Int, id: Int, r: RuleContainer, dataFrameRuleParts: RDD[(RDFTriple, Int, Int)], sc: SparkContext, sqlContext: SQLContext): RDD[(RDFTriple, Int, Int)] = {

      var out: DataFrame = null
      var OUT: RDD[(RDFTriple, Int, Int)] = dataFrameRuleParts
      //var count2:RDD[(String, Int)] = null 
      var path = new File("test_table/")
      var temp = 0

      val tpAr = r.getRule()
      var tpArString = ""
      var stringSELECT = ""
      for (tp <- 0 to tpAr.length - 1) {
        tpArString += tpAr(tp).toString
        stringSELECT += "tp" + tp + ", "

      }
      tpArString = tpArString.replace(" ", "_").replace("?", "_")
      stringSELECT += "tp" + tpAr.length

      var z: Try[Row] = null
      if ((tpAr.length != maxLen - 1) && (temp == 0)) {
        var a = kb.addDanglingAtom(c, id, minHC, r, sc, sqlContext)

        z = Try(a.first())
        if ((!(z.isFailure)) && (z.isSuccess)) {

          out = a

        }

      }

      var b = kb.addClosingAtom(c, id, minHC, r, sc, sqlContext)

      var t = Try(b.first)

      if ((!(t.isFailure)) && (t.isSuccess) && (temp == 0)) {

        if (out == null) {
          out = b
        } else {
          out = out.unionAll(b)

        }

      }

      var count: RDD[(String, Int)] = null
      var o: RDD[(RDFTriple, Int, Int)] = null

      if (((!(t.isFailure)) && (t.isSuccess)) || ((z != null) && (!(z.isFailure)) && (z.isSuccess))) {
        count = out.rdd.map(x => (x(r.getRule().length + 1).toString(), 1)).reduceByKey(_ + _)

        o = count.map(q => (q._1.split("\\s+"), q._2)).map { token =>
          Tuple3(RDFTriple(token._1(0), token._1(1), token._1(2)), token._2, token._1(3).toInt)
        }.filter(n1 => (n1._2 >= (kb.getRngSize(n1._1.predicate) * minHC)))

        if (OUT == null) {
          OUT = o
        } else {
          OUT = OUT.union(o)
        }

        if (tpAr.length < maxLen) {
          var namesFolder = o.map(x => (tpArString + "" + x._1.toString().replace(" ", "_").replace("?", "_"), x._1.toString() + "  " + id.toString())).collect
          var delFiles = new File(hdfsPath + "permanent" + (c - 2) + "/")

          while (delFiles.listFiles() != null) {
            deleteRecursive(delFiles)
          }

          out.registerTempTable("outTable")
          for (n <- namesFolder) {
            var tempDF = sqlContext.sql("SELECT " + stringSELECT + " FROM outTable WHERE outTable.key = '" + n._2 + "'")

            tempDF.write.parquet(hdfsPath + "permanent" + c + "/" + n._1)

          }
        }
      }

      return OUT

    }

    def acceptedForOutput(outMap: Map[String, ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]], r: RuleContainer, minConf: Double, k: KB, sc: SparkContext, sqlContext: SQLContext): Boolean = {

      //if ((!(r.closed())) || (r.getPcaConfidence(k, sc, sqlContext) < minConf)) {
      if ((!(r.closed())) || (r.getPcaConfidence() < minConf)) {
        return false

      }

      var parents: ArrayBuffer[RuleContainer] = r.parentsOfRule(outMap, sc)
      if (r.getRule.length > 2) {
        for (rp <- parents) {
          if (r.getPcaConfidence() <= rp.getPcaConfidence()) {
            return false
          }

        }
      }

      return true
    }

    def sort(tp: ArrayBuffer[RDFTriple]): ArrayBuffer[RDFTriple] = {
      var out = ArrayBuffer(tp(0))
      var temp = new ArrayBuffer[Tuple2[String, RDFTriple]]

      for (i <- 1 to tp.length - 1) {
        var tempString: String = tp(i).predicate + tp(i).subject + tp(i).`object`
        temp += Tuple2(tempString, tp(i))

      }
      temp = temp.sortBy(_._1)
      for (t <- temp) {
        out += t._2
      }

      return out
    }

  }

  def main(args: Array[String]) = {
    val know = new KB()

    val sparkSession = SparkSession.builder

      .master("local[*]")
      .appName("AMIESpark example")

      .getOrCreate()

    if (args.length < 2) {
      System.err.println(
        "Usage: Triple reader <input> <output>")
      System.exit(1)
    }

    val input = args(0)
    val outputPath: String = args(1)
    val hdfsPath: String = outputPath + "/"

    val sc = sparkSession.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    know.sethdfsPath(hdfsPath)
    know.setKbSrc(input)

    know.setKbGraph(RDFGraphLoader.loadFromFile(know.getKbSrc(), sc, 2))
    know.setDFTable(DfLoader.loadFromFileDF(know.getKbSrc, sc, sqlContext, 2))

    val algo = new Algorithm(know, 0.01, 3, 0.1, hdfsPath)

    var output = algo.ruleMining(sc, sqlContext)

    var outString = output.map { x =>
      var rdfTrp = x.getRule()
      var temp = ""
      for (i <- 0 to rdfTrp.length - 1) {
        if (i == 0) {
          temp = rdfTrp(i) + " <= "
        } else {
          temp += rdfTrp(i) + " \u2227 "
        }
      }
      temp = temp.stripSuffix(" \u2227 ")
      temp
    }.toSeq
    var rddOut = sc.parallelize(outString)

    rddOut.saveAsTextFile(outputPath + "/testOut")

    sc.stop

  }

}