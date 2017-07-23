package net.sansa_stack.ml.spark.mining.amieSpark

import net.sansa_stack.ml.spark.mining.amieSpark._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import KBObject.KB
import scala.collection.mutable.Map

object Rules {

  class RuleContainer() extends Serializable {
    val name: String = ""

    var rule: ArrayBuffer[RDFTriple] = new ArrayBuffer
    var sortedRule: ArrayBuffer[RDFTriple] = new ArrayBuffer

    var headKey = null;
    var support: Long = -1;
    var hashSupport = 0;
    var parent: RuleContainer = null;
    var bodySize = -1.0;
    var highestVariable: Char = 'b';
    var pcaBodySize = 0.0;
    var pcaConfidence = 0.0;
    var _stdConfidenceUpperBound = 0.0;
    var _pcaConfidenceUpperBound = 0.0;
    var _pcaConfidenceEstimation = 0.0;
    var _belowMinimumSupport = false;
    var _containsQuasiBindings = false;
    var ancestors = new ArrayBuffer[(String, String, String)]()
    var highestVariableSuffix = 0;
    var variableList: ArrayBuffer[String] = ArrayBuffer("?a", "?b")
    var sizeHead: Double = 0.0

    def getRule(): ArrayBuffer[RDFTriple] = {
      return this.rule
    }

    def getSortedRule(): ArrayBuffer[RDFTriple] = {
      return this.sortedRule
    }

    /**initializes rule, support, bodySize and sizeHead*/
    def initRule(x: ArrayBuffer[RDFTriple], k: KB, sc: SparkContext, sqlContext: SQLContext) {
      this.rule = x

      calcSupport(k, sc, sqlContext)
      setBodySize(k, sc, sqlContext)
      setSizeHead(k)

    }

    def setRule(threshold: Double, supp: Long, p: RuleContainer, x: ArrayBuffer[RDFTriple], y: ArrayBuffer[RDFTriple], k: KB, sc: SparkContext, sqlContext: SQLContext) {
      this.rule = x

      this.sortedRule = y

      this.support = supp

      setBodySize(k, sc, sqlContext)
      setSizeHead(k)

      this.parent = p

      this.setPcaConfidence(threshold, k, sc, sqlContext)
    }

    def setRuleArBuf(tp: ArrayBuffer[RDFTriple]) {
      this.rule = tp
    }

    /**returns ArrayBuffer with every triplePattern of the body as a RDFTriple*/

    def hc(): Double = {
      if (this.bodySize < 1) {
        return -1.0

      }

      return ((this.support) / (this.sizeHead))

    }

    /**
     * returns number of instantiations of the predicate of the head
     *
     * @param k knowledge base
     *
     */
    def setSizeHead(k: KB) {
      val headElem = this.rule(0)

      var rel = headElem.predicate

      var out = k.sizeOfRelation(rel)

      this.sizeHead = out
    }

    /**
     * returns number of instantiations of the relations in the body
     *
     * @param k knowledge base
     * @param sc spark context
     *
     */

    def setBodySize(k: KB, sc: SparkContext, sqlContext: SQLContext) {
      if ((this.rule.length - 1) > 1) {
        var body = this.rule.clone
        body.remove(0)
        var mapList = k.cardinality(body, sc, sqlContext)

        this.bodySize = mapList.count()
      }

    }

    /**
     * returns the support of a rule, the number of instantiations of the rule
     *
     * @param k knowledge base
     * @param sc spark context
     *
     */

    def calcSupport(k: KB, sc: SparkContext, sqlContext: SQLContext) {

      if (this.rule.length > 1) {

        val mapList = k.cardinality(this.rule, sc, sqlContext)

        this.support = mapList.count()
      }
    }
    /**returns the length of the body*/

    def bodyLength(): Int = {
      var x = this.rule.length - 1
      return x

    }

    def usePcaApprox(tparr: ArrayBuffer[RDFTriple]): Boolean = {

      var maptp: Map[String, Int] = Map()
      if (tparr.length <= 2) {
        return false
      }

      for (x <- tparr) {

        if (!(maptp.contains(x._1))) {
          maptp += (x._1 -> 1)
        } else {
          var counter: Int = maptp.remove(x._1).get + 1
          if (counter > 2) { return false }

          maptp += (x._1 -> counter)
        }

        /**checking map for placeholder for the object*/
        if (!(maptp.contains(x._3))) {
          maptp += (x._3 -> 1)
        } else {
          var counter: Int = maptp.remove(x._3).get + 1
          if (counter > 2) { return false }

          maptp += (x._3 -> counter)
        }

      }
      var counter = 0
      var isLength = false
      maptp.foreach { x =>
        counter += 1

        if (x._2 == tparr.length) {
          isLength = true
        }
      }
      if ((isLength) && (counter == 2)) {
        return false
      }
      var max = 0
      maptp.foreach { value =>
        if (value._2 == 1) { return false }
        if (max < value._2) { max = value._2 }
      }
      if (max > 2) {
        return false
      }
      return true

    }

    def setPcaConfidence(threshold: Double, k: KB, sc: SparkContext, sqlContext: SQLContext) {

      var tparr = this.rule.clone
      val usePcaA = usePcaApprox(tparr)

      /**
       * when it is expensive to compute pca approximate pca
       */
      if (usePcaA) {

        set_pcaConfidenceEstimation(k: KB)
        if (this._pcaConfidenceEstimation > threshold) {

          this.pcaBodySize = k.cardPlusnegativeExamplesLength(tparr, this.support, sc, sqlContext)

          if (this.pcaBodySize > 0.0) {
            this.pcaConfidence = (support / pcaBodySize)
          }
        }

      } else {

        this.pcaBodySize = k.cardPlusnegativeExamplesLength(tparr, this.support, sc, sqlContext)
        if (this.pcaBodySize > 0.0) {
          this.pcaConfidence = (support / pcaBodySize)
        }

      }

    }

    def getPcaConfidence(): Double = {
      return this.pcaConfidence
    }

    def setPcaBodySize(k: KB, sc: SparkContext) {
      val tparr = this.rule

      val out = k.cardPlusnegativeExamplesLength(tparr, sc)

      this.pcaBodySize = out

    }

    def set_pcaConfidenceEstimation(k: KB) {
      var r = this.rule.clone
      var r0 = (k.overlap(r(1).predicate, r(0).predicate, 0)) / (k.functionality(r(1).predicate))
      var rrest: Double = 1.0

      for (i <- 2 to r.length - 1) {
        rrest = rrest * (((k.overlap(r(i - 1).predicate, r(i).predicate, 2)) / (k.getRngSize(r(i - 1).predicate))) * ((k.inverseFunctionality(r(i).predicate)) / (k.functionality(r(i).predicate))))

      }

      this._pcaConfidenceEstimation = r0 * rrest

    }

    def parentsOfRule(outMap: Map[String, ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]], sc: SparkContext): ArrayBuffer[RuleContainer] = {
      // TODO: create new rules with body in alphabetical order     
      var parents = ArrayBuffer(this.parent)
      val r = this.sortedRule.clone

      if (outMap.get(r(0).predicate) == None) { return parents }
      var rel = outMap.get(r(0).predicate).get

      var tp: ArrayBuffer[RDFTriple] = new ArrayBuffer

      var filtered = rel.filter(x => (x._1.length == r.length - 1))

      /* 
             for (f <- filtered){
               var bool = true
               for (ff <- f._1){
                 if (!(r.contains(ff))){
                   bool = false
                 }
               }
               if (bool){
                 parents += f._2
               }
             }*/

      for (l <- 1 to r.length - 1) {
        if (!(filtered.isEmpty)) {

          tp = r.clone()
          tp.remove(l)
          var x = partParents(tp, filtered, sc)
          parents ++= x._1
          filtered = x._2
        }
      }

      return parents

    }

    def closed(): Boolean = {

      var tparr = this.rule
      var maptp: Map[String, Int] = Map()
      if (tparr.length == 1) {
        return false
      }

      for (x <- tparr) {

        if (!(maptp.contains(x._1))) {

          maptp += (x._1 -> 1)
        } else {

          maptp.put(x._1, (maptp.get(x._1).get + 1)).get
        }

        /**checking map for placeholder for the object*/
        if (!(maptp.contains(x._3))) {

          maptp += (x._3 -> 1)
        } else {
          maptp.put(x._3, (maptp.get(x._3).get + 1)).get
        }

      }

      maptp.foreach { value => if (value._2 == 1) return false }

      return true

    }

    def notClosed(): Option[ArrayBuffer[String]] = {
      var maxVar: Char = 'a'
      var varArBuff = new ArrayBuffer[String]

      var tparr = this.rule
      var maptp: Map[String, Int] = Map()
      if (tparr.length == 1) {

        return Some(ArrayBuffer(tparr(0)._1, tparr(0)._3))
      }

      for (x <- tparr) {

        if (!(maptp.contains(x._1))) {
          varArBuff += x._1
          if (x._1(1) > maxVar) {
            maxVar = x._1(1)
          }
          maptp += (x._1 -> 1)
        } else {

          maptp.put(x._1, (maptp.get(x._1).get + 1)).get
        }

        /**checking map for placeholder for the object*/
        if (!(maptp.contains(x._3))) {
          varArBuff += x._3

          if (x._3(1) > maxVar) {
            maxVar = x._3(1)
          }
          maptp += (x._3 -> 1)
        } else {
          maptp.put(x._3, (maptp.get(x._3).get + 1)).get
        }

      }
      var out: ArrayBuffer[String] = new ArrayBuffer
      this.variableList = varArBuff
      maptp.foreach { value => if (value._2 == 1) out += value._1 }

      this.highestVariable = maxVar
      if (out.isEmpty) {
        return None
      } else {
        return Some(out)
      }

    }

    def getHighestVariable(): Char = {
      return this.highestVariable
    }

    def getVariableList(): ArrayBuffer[String] = {
      return this.variableList
    }

    def partParents(triples: ArrayBuffer[RDFTriple], arbuf: ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)], sc: SparkContext): (ArrayBuffer[RuleContainer], ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]) = {
      var out1: ArrayBuffer[RuleContainer] = new ArrayBuffer
      var out2: ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)] = new ArrayBuffer

      var out: (ArrayBuffer[RuleContainer], ArrayBuffer[(ArrayBuffer[RDFTriple], RuleContainer)]) = (out1, out2)
      if (triples.length <= 1) {
        return out
      }
      var triplesCardcombis: ArrayBuffer[ArrayBuffer[RDFTriple]] = new ArrayBuffer

      //var rdd =sc.parallelize(arbuf.toSeq)
      //out ++= rdd.filter(x => (sameRule(triples, x._1))).map(y => y._2).collect

      for (x <- arbuf) {
        if (sameRule(triples, x._1)) {
          out1 += x._2

        } else out2 += x
      }

      /* var rdd = sc.parallelize(arbuf.toSeq)
          var pq = rdd.map{ x=>
           if (sameRule(triples, x._1)){
             ("out1", x._2) 
             
           }
           else ("out2",x)
         }.groupByKey()
         * 
         */

      out = (out1, out2)
      return out

    }

    def rdTpEquals(a: RDFTriple, b: RDFTriple): Boolean = {
      if ((a._1 == b._1) && (a._2 == b._2) && (a._3 == b._3)) {
        return true

      } else {
        return false
      }
    }

    def rdfTpArrEquals(a: ArrayBuffer[RDFTriple], b: ArrayBuffer[RDFTriple]): Boolean = {
      if (!(a.length == b.length)) {
        return false
      }

      for (i <- 0 to a.length - 1) {
        if (!(rdTpEquals(a(i), b(i)))) {
          return false

        }
      }
      return true
    }

    def sameRule(a: ArrayBuffer[RDFTriple], b: ArrayBuffer[RDFTriple]): Boolean = {
      var varMap: Map[String, String] = Map()
      if (a.length != b.length) { return false }
      for (i <- 0 to a.length - 1) {

        if (a(i)._2 != b(i)._2) { return false }
        if (!(varMap.contains(a(i)._1))) {
          varMap += (a(i)._1 -> b(i)._1)
        }

        if (!(varMap.contains(a(i)._3))) {
          varMap += (a(i)._3 -> b(i)._3)
        }

        if (!((varMap.get(a(i)._1) == Some(b(i)._1)) && (varMap.get(a(i)._3) == Some(b(i)._3)))) {
          return false
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

    //end

  }

}