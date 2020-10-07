package net.sansa_stack.ml.spark.mining.amieSpark

import java.io.File

import scala.collection.mutable.{ ArrayBuffer, Map }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.functions.udf

import net.sansa_stack.ml.spark.mining.amieSpark.Rules.RuleContainer

object KBObject {
  case class Atom(rdf: RDFTriple)
  class KB() extends Serializable {
    var kbSrc: String = ""

    var kbGraph: RDFGraph = _
    var dfTable: DataFrame = _

    var dfMap: Map[String, DataFrame] = Map()

    var hdfsPath: String = ""

    var subject2predicate2object: Map[String, Map[String, Map[String, Int]]] = Map()
    var predicate2object2subject: Map[String, Map[String, Map[String, Int]]] = Map()
    var object2subject2predicate: Map[String, Map[String, Map[String, Int]]] = Map()
    var predicate2subject2object: Map[String, Map[String, Map[String, Int]]] = Map()
    var object2predicate2subject: Map[String, Map[String, Map[String, Int]]] = Map()
    var subject2object2predicate: Map[String, Map[String, Map[String, Int]]] = Map()

    var subjectSize: Map[String, Int] = Map()
    var objectSize: Map[String, Int] = Map()
    var relationSize: Map[String, Int] = Map()

    var subject2subjectOverlap: Map[String, Map[String, Int]] = Map()
    var subject2objectOverlap: Map[String, Map[String, Int]] = Map()
    var object2objectOverlap: Map[String, Map[String, Int]] = Map()

    /** Identifiers for the overlap maps */
    val SUBJECT2SUBJECT: Int = 0

    val SUBJECT2OBJECT: Int = 2

    val OBJECT2OBJECT: Int = 4

    def sethdfsPath(hdfsP: String) {
      this.hdfsPath = hdfsP
    }

    def setDfMap(name: String, df: DataFrame) {
      this.dfMap += name -> df
    }

    def tpArString(tpAr: ArrayBuffer[RDFTriple]): String = {
      var str = ""
      for (tp <- tpAr) {
        str += tp.toString()
      }

      str = str.replace(" ", "_").replace("?", "_")
      str
    }

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

    def getRngSize(rel: String): Double = {

      this.predicate2object2subject.get(rel).get.size
    }

    def setKbSrc(x: String) {
      this.kbSrc = x
    }
    def getKbSrc: String = {

      this.kbSrc
    }

    def getKbGraph: RDFGraph = {
      this.kbGraph

    }

    // TODO: think about Graph representation
    def setKbGraph(x: RDFGraph) {
      this.kbGraph = x
      val graph = x.triples.collect
      for (i <- graph) {
        add(i)
      }
      buildOverlapTables()

    }

    def setDFTable(x: DataFrame) {
      this.dfTable = x
    }

    def add(fst: String, snd: String, thrd: String, toMaps: Map[String, Map[String, Map[String, Int]]]): Boolean = {
      var out = true
      if (toMaps.get(fst).isEmpty) {
        toMaps += (fst -> Map(snd -> Map(thrd -> 0)))
        out = false
      } else if (toMaps.get(fst).get.get(snd).isEmpty) {
        toMaps.get(fst).get += (snd -> Map(thrd -> 0))
        out = false
      } else if (toMaps.get(fst).get.get(snd).get.get(thrd).isEmpty) {
        toMaps.get(fst).get.get(snd).get += (thrd -> 0)
        out = false

      }

      out
    }

    /**
     * function to fill the Maps subject2predicate2object,... ,sizes and Overlaps
     *
     * @param tp triple which subject, object and predicate is put in Maps and are counted for the sizes
     *
     */

    def add(tp: RDFTriple) {

      val subject = tp.subject
      val relation = tp.predicate
      val o = tp.`object`
      // filling the to to to maps
      if (!(add(subject, relation, o, this.subject2predicate2object))) {
        add(relation, o, subject, this.predicate2object2subject)
        add(o, subject, relation, this.object2subject2predicate)
        add(relation, subject, o, this.predicate2subject2object)
        add(o, relation, subject, this.object2predicate2subject)
        add(subject, o, relation, this.subject2object2predicate)
      }

      // filling the sizes
      if (this.subjectSize.get(subject).isEmpty) {
        this.subjectSize += (subject -> 1)
      } else {
        var subSize: Int = this.subjectSize.remove(subject).get + 1

        this.subjectSize += (subject -> subSize)
      }

      if (this.relationSize.get(relation).isEmpty) {
        this.relationSize += (relation -> 1)
      } else {
        var relSize: Int = this.relationSize.remove(relation).get + 1

        this.relationSize += (relation -> relSize)
      }

      if (this.objectSize.get(o).isEmpty) {
        this.objectSize += (o -> 1)
      } else {
        var obSize: Int = this.objectSize.remove(o).get + 1

        this.objectSize += (o -> obSize)
      }

      // filling the overlaps

      if (this.subject2subjectOverlap.get(relation).isEmpty) {
        subject2subjectOverlap += (relation -> Map())
      }

      if (this.subject2objectOverlap.get(relation).isEmpty) {
        subject2objectOverlap += (relation -> Map())
      }

      if (this.object2objectOverlap.get(relation).isEmpty) {
        object2objectOverlap += (relation -> Map())
      }

    }

    /**
     * returns number of instantiations of relation
     *
     * @param rel Relation
     * @param number of instantiations
     *
     *
     */

    def sizeOfRelation(rel: String): Int = {
      if (this.relationSize.get(rel).isEmpty) {
        return 0

      }
      this.relationSize.get(rel).get

    }

    /* TODO
    * Functionality
    * bulidOverlapTable
    * */

    def relationsSize(): Int = {

      this.relationSize.size
    }

    /**
     * Returns the number of entities in the database.
     * @return
     */
    def entitiesSize(): Int = {
      var x = this.subjectSize.size
      var y = this.objectSize.size

      (x + y)
    }

    /**
     * @author AMIE+ Team
     */

    def buildOverlapTables() {
      for (r1 <- this.relationSize.keys) {
        val subjects1 = predicate2subject2object.get(r1).get.keys.toSet

        val objects1 = predicate2object2subject.get(r1).get.keys.toSet

        for (r2 <- this.relationSize.keys) {
          val subjects2 = predicate2subject2object.get(r2).get.keys.toSet

          val objects2 = predicate2object2subject.get(r2).get.keys.toSet

          if (!r1.equals(r2)) {
            var ssoverlap: Int = computeOverlap(subjects1, subjects2)
            subject2subjectOverlap.get(r1).get.put(r2, ssoverlap)
            subject2subjectOverlap.get(r2).get.put(r1, ssoverlap)
          } else {
            subject2subjectOverlap.get(r1).get.put(r1, subjects2.size)
          }

          var soverlap1: Int = computeOverlap(subjects1, objects2)
          subject2objectOverlap.get(r1).get.put(r2, soverlap1)
          var soverlap2: Int = computeOverlap(subjects2, objects1)
          subject2objectOverlap.get(r2).get.put(r1, soverlap2)

          if (!r1.equals(r2)) {
            var oooverlap: Int = computeOverlap(objects1, objects2)
            object2objectOverlap.get(r1).get.put(r2, oooverlap)
            object2objectOverlap.get(r2).get.put(r1, oooverlap)
          } else {
            object2objectOverlap.get(r1).get.put(r1, objects2.size)
          }
        }
      }
    }

    /**
     * @author AMIE+ Team
     *
     * @param s1 set with subjects or objects
     * @param s2 set with subjects or objects
     * @out overlap of s1 and s2
     */
    def computeOverlap(s1: Set[String], s2: Set[String]): Int = {
      var overlap: Int = 0
      for (r <- s1) {
        if (s2.contains(r)) {
          overlap += 1
        }
      }

      overlap
    }

    // ---------------------------------------------------------------------------
    // Functionality
    // ---------------------------------------------------------------------------

    /**
     * @author AMIE+ Team
     * It returns the harmonic functionality of a relation, as defined in the PARIS paper
     * https://www.lri.fr/~cr/Publications_Master_2013/Brigitte_Safar/p157_fabianmsuchanek_vldb2011.pdf
     *
     */
    def functionality(relation: String): Double = {
      /* if (relation.equals(EQUALSbs)) {
         return 1.0; */

      if (this.predicate2subject2object.get(relation).isEmpty) { return 0.0 }
      var a: Double = this.predicate2subject2object.get(relation).get.size
      var b: Double = this.relationSize.get(relation).get
      (a / b)

    }

    /**
     * @author AMIE+ Team
     * Returns the harmonic inverse functionality, as defined in the PARIS paper
     * https://www.lri.fr/~cr/Publications_Master_2013/Brigitte_Safar/p157_fabianmsuchanek_vldb2011.pdf
     *
     */
    def inverseFunctionality(relation: String): Double = {
      /* if (relation.equals(EQUALSbs)) {
       return 1.0
       } */
      var a: Double = this.predicate2object2subject.get(relation).get.size
      var b: Double = this.relationSize.get(relation).get
      (a / b)

    }

    /**
     * Determines whether a relation is functional, i.e., its harmonic functionality
     * is greater than its inverse harmonic functionality.
     * @param relation
     * @return
     * @author AMIE+ Team
     */
    def isFunctional(relation: String): Boolean = {
      functionality(relation) >= inverseFunctionality(relation)
    }

    /**
     *  @author AMIE+ Team
     * It returns the functionality or the inverse functionality of a relation.
     * @param relation
     * @param inversed If true, the method returns the inverse functionality, otherwise
     * it returns the standard functionality.
     * @return
     *
     */
    def functionality(relation: String, inversed: Boolean): Double = {
      if (inversed) {
        inverseFunctionality(relation)
      } else {
        functionality(relation)
      }
    }

    /**
     * @author AMIE+ Team
     * It returns the functionality or the inverse functionality of a relation.
     * @param inversed If true, the method returns the functionality of a relation,
     * otherwise it returns the inverse functionality.
     * @return
     *
     */
    def inverseFunctionality(relation: String, inversed: Boolean): Double = {
      if (inversed) {
        functionality(relation)
      } else {
        inverseFunctionality(relation)
      }
    }

    /**
     * returns ArrayBuffer with successful maps of the '?'
     * placeholders  in a rule to real facts in the KB
     * length of maplist is the number of instantiations of a rule
     *
     * @param triplesCard rule as an ArrayBuffer of RDFTriples, triplesCard(0)
     * is the head of the rule
     * @param sc spark context
     *
     */

    // ----------------------------------------------------------------
    // Statistics
    // ----------------------------------------------------------------

    def overlap(relation1: String, relation2: String, overlap: Int): Double = {
      overlap match {
        case SUBJECT2SUBJECT =>
          if (subject2subjectOverlap.get(relation1).isDefined && (!(subject2subjectOverlap.get(relation1).get.get(relation2).isEmpty))) {
            subject2subjectOverlap.get(relation1).get.get(relation2).get
          } else 0.0
        case SUBJECT2OBJECT =>
          if ((!(subject2objectOverlap.get(relation1).isEmpty)) && (!(subject2objectOverlap.get(relation1).get.get(relation2).isEmpty))) {
            subject2objectOverlap.get(relation1).get.get(relation2).get
          } else 0.0
        case OBJECT2OBJECT =>
          if ((!(object2objectOverlap.get(relation1).isEmpty)) && (!(object2objectOverlap.get(relation1).get.get(relation2).isEmpty))) {
            object2objectOverlap.get(relation1).get.get(relation2).get
          } else 0.0
      }
    }

    /**
     * @author AMIE+ team
     * It returns the number of distinct instance of one of the arguments (columns)
     * of a relation.
     * @param relation
     * @param column. Subject or Object
     * @return
     */

    def relationColumnSize(rel: String, elem: String): Int = {
      elem match {
        case "subject" =>
          predicate2subject2object.get(rel).get.size

        case "object" =>
          predicate2object2subject.get(rel).get.size

      }

    }

    // TODO: better than cardinality

    def bindingExists(triplesCard: ArrayBuffer[RDFTriple]): Boolean = {
      val k = this.kbGraph
      if (triplesCard.isEmpty) {
        return true
      }
      if (triplesCard.length == 1) {
        var last = triplesCard(0)
        if (last.subject.startsWith("?")) {
          return (k.find(None, Some(last.predicate), Some(last.`object`)).collect.length) > 0
        } else if (triplesCard(0).`object`.startsWith("?")) {
          return (k.find(Some(last.subject), Some(last.predicate), None).collect.length) > 0
        } else {
          return false
        }
      }

      var mapList: ArrayBuffer[Map[String, String]] = new ArrayBuffer

      var min: RDFTriple = triplesCard(0)
      var minSize = this.relationSize.get(triplesCard(0).predicate).get
      var index = 0

      for (i <- 1 until triplesCard.length) {
        if (this.relationSize.get(triplesCard(i).predicate).get < minSize) {
          minSize = this.relationSize.get(triplesCard(i).predicate).get
          min = triplesCard(i)
          index = i

        }
      }

      var a = min._1
      var b = min._3

      var x: Array[RDFTriple] = Array()

      if (!(a.startsWith("?"))) {
        x = k.find(Some(a), Some(min.predicate), None).collect
      } else if (!(b.startsWith("?"))) {
        x = k.find(None, Some(min.predicate), Some(b)).collect
      } else {
        x = k.find(None, Some(min.predicate), None).collect
      }

      // x.foreach(println)
      triplesCard.remove(index)

      for (i <- x) {
        var temp: ArrayBuffer[RDFTriple] = new ArrayBuffer
        var exploreFurther = true
        for (j <- triplesCard) {

          var test = true
          var atestLeft = (this.predicate2subject2object.get(j._2).get.get(i._1).isEmpty)
          var atestRight = (this.predicate2object2subject.get(j._2).get.get(i._1).isEmpty)

          var btestRight = (this.predicate2object2subject.get(j._2).get.get(i._3).isEmpty)
          var btestLeft = (this.predicate2subject2object.get(j._2).get.get(i._3).isEmpty)

          if (((a.startsWith("?") && (b.startsWith("?")))) &&
            (((j._1 == a) && (j._3 == b) && (!(atestLeft)) && (!(btestRight))) ||
              ((j._1 == b) && (j._3 == a) && (!(atestRight)) && (!(btestLeft))))) {
            test = false

          }

          if ((!(j._1.startsWith("?"))) && (!(j._3.startsWith("?")))) {
            test = false

          }

          if (test) {

            if ((a.startsWith("?")) && ((j._1 == a) && (!(atestLeft)))) {
              temp += RDFTriple(i._1, j._2, j._3)

            } else if ((a.startsWith("?")) && ((j._3 == a) && (!(atestRight)))) {
              temp += RDFTriple(j._1, j._2, i._1)

            } else if ((b.startsWith("?")) && ((j._3 == b) && (!(btestRight)))) {
              temp += RDFTriple(j._1, j._2, i._3)

            } else if ((b.startsWith("?")) && ((j._1 == b) && (!(btestLeft)))) {
              temp += RDFTriple(i._3, j._2, j._3)
            } else if ((b.startsWith("?")) && (((j._3 == b) && (btestRight)) || ((j._1 == b) && (btestLeft)))) {
              exploreFurther = false
            } else if ((a.startsWith("?")) && (((j._1 == a) && (atestLeft)) || ((j._3 == a) && (atestRight)))) {
              exploreFurther = false
            } else {

              temp += j

            }

          }

        }

        if (exploreFurther) {
          if (bindingExists(temp)) {

            return true

          }
        }

      }

      false
    }

    def varCount(tpAr: ArrayBuffer[RDFTriple]): ArrayBuffer[(String, String)] = {

      var out2: ArrayBuffer[(String, String)] = new ArrayBuffer

      for (i <- tpAr) {
        if (!(out2.contains(Tuple2(i.subject, i.predicate)))) {
          out2 += Tuple2(i.subject, i.predicate)
        }

        if (!(out2.contains(Tuple2(i.predicate, i.`object`)))) {
          out2 += Tuple2(i.predicate, i.`object`)
        }

      }

      out2
    }
    def countProjectionQueriesDF(posit: Int, id: Int, operator: String, minHC: Double, tpAr: ArrayBuffer[RDFTriple], RXY: ArrayBuffer[(String, String)], sc: SparkContext, sqlContext: SQLContext): DataFrame =
      {

        val threshold = minHC * this.relationSize.get(tpAr(0).predicate).get
        var relations: ArrayBuffer[String] = new ArrayBuffer

        val variables = varCount(tpAr)

        var whole: DataFrame = null
        var counter = 0

        var tpArDF: DataFrame = null
        if (posit == 0) {
          val DF = this.dfTable

          DF.registerTempTable("table")

          tpArDF = sqlContext.sql("SELECT rdf AS tp0 FROM table WHERE rdf.predicate = '" + tpAr(0).predicate + "'")

        } else {

          tpArDF = dfMap.get(calcName(tpAr)).get
        }

        tpArDF.cache()
        this.relationSize.foreach { x =>
          for (i <- RXY) {
            var go = true
            var temp = tpAr.clone()
            temp += RDFTriple(i._1, x._1, i._2)

            if (operator == "OD") {
              if (variables.contains(Tuple2(i._1, x._1))) {
                go = false

              } else if (variables.contains(Tuple2(x._1, i._2))) {
                go = false
              }
            } else if (operator == "OC") {
              if (tpAr.contains(temp.last)) {
                go = false
              }
            }

            if ((this.bindingExists(temp.clone())) && (go)) {

              var part = this.cardinalityQueries(id, tpArDF, temp, sc, sqlContext)

              if (whole == null) {
                whole = part
              } else {
                whole = whole.unionAll(part)
              }
              counter += 1
            }
          }

        }

        whole

      }

    def cardinalityQueries(id: Int, tpArDF: DataFrame, wholeAr: ArrayBuffer[RDFTriple], sc: SparkContext, sqlContext: SQLContext): DataFrame = {
      val DF = this.dfTable
      var tpMap: Map[String, ArrayBuffer[(Int, String)]] = Map()
      DF.registerTempTable("table")
      tpArDF.registerTempTable("tpArTable")

      var w = sqlContext.sql("SELECT rdf AS tp" + (wholeAr.length - 1) + " FROM table WHERE rdf.predicate = '" + (wholeAr.last).predicate + "'")
      w.registerTempTable("newColumn")

      var v = sqlContext.sql("SELECT * FROM tpArTable JOIN newColumn")

      var varAr: ArrayBuffer[String] = new ArrayBuffer
      var checkMap: Map[Int, (String, String)] = Map()
      var checkSQLSELECT = "SELECT "

      for (i <- wholeAr.indices) {
        var a = wholeAr(i).subject
        var b = wholeAr(i)._3

        checkSQLSELECT += "tp" + i + ", "

        varAr ++= ArrayBuffer(a, b)
        checkMap += (i -> Tuple2(a, b))

        if (!(tpMap.contains(a))) {
          tpMap += ((a) -> ArrayBuffer(Tuple2(i, "subject")))

        } else {
          var temp = tpMap.get(a).get
          temp += Tuple2(i, "subject")
          tpMap.put(a, temp)
        }
        if (!(tpMap.contains(b))) {
          tpMap += ((b) -> ArrayBuffer(Tuple2(i, "`object`")))

        } else {
          var temp = tpMap.get(b).get
          temp += Tuple2(i, "`object`")
          tpMap.put(b, temp)

        }
      }
      checkSQLSELECT = checkSQLSELECT.stripSuffix(", ")

      var cloneTpAr = wholeAr.clone()

      var removedMap: Map[String, ArrayBuffer[(Int, String)]] = Map()

      varAr = varAr.distinct
      var checkSQLWHERE = "WHERE "
      checkMap.foreach { ab =>
        var a = ab._2._1
        var b = ab._2._2

        if (varAr.contains(a)) {

          varAr -= a
          var x = tpMap.get(a).get
          for (k <- x) {
            if (k._1 != ab._1) {
              checkSQLWHERE += "tp" + ab._1 + ".subject = tp" + k._1 + "." + k._2 + " AND "

            }

          }

        }

        if (varAr.contains(b)) {

          varAr -= b
          var y = tpMap.get(b).get

          for (k <- y) {
            if (k._1 != ab._1) {
              checkSQLWHERE += "tp" + ab._1 + ".`object` = tp" + k._1 + "." + k._2 + " AND "

            }
          }

        }

      }

      checkSQLWHERE = checkSQLWHERE.stripSuffix(" AND ")
      var seq: Seq[String] = Seq((wholeAr.last.toString() + "  " + id.toString))
      import sqlContext.implicits._
      var key: DataFrame = seq.toDF("key")

      v.registerTempTable("t")
      var last = sqlContext.sql(checkSQLSELECT + " FROM t " + checkSQLWHERE)

      this.setDfMap(calcName(wholeAr), last)

      last.registerTempTable("lastTable")
      key.registerTempTable("keyTable")
      var out = sqlContext.sql(checkSQLSELECT + ", keyTable.key FROM lastTable JOIN keyTable")

      out

    }

    /**
     * used in setBodySize and calcSupport
     */

    def cardinality(tpAr: ArrayBuffer[RDFTriple], sc: SparkContext, sqlContext: SQLContext): DataFrame = {
      println(s"computing cardinality for ${tpAr.mkString(",")} ...")
      var name = calcName(tpAr)

      if (dfMap.contains(name)) {
        dfMap.get(name).get
      } else {
        val DF = this.dfTable
        var tpMap: Map[String, ArrayBuffer[(Int, String)]] = Map()
        DF.registerTempTable("table")

        var v = sqlContext.sql("SELECT rdf AS tp0 FROM table WHERE rdf.predicate = '" + tpAr(0).predicate + "'")

        for (k <- 1 until tpAr.length) {
          var w = sqlContext.sql("SELECT rdf AS tp" + k + " FROM table WHERE rdf.predicate = '" + tpAr(k).predicate + "'")
          w.registerTempTable("newColumn")

          var tempO = v
          tempO.registerTempTable("previous")

          var sqlString = ""
          for (re <- 0 until k) {
            sqlString += "previous.tp" + re + ", "
          }

          v = sqlContext.sql("SELECT " + sqlString + "newColumn.tp" + k + " FROM previous JOIN newColumn")

        }

        var varAr: ArrayBuffer[String] = new ArrayBuffer
        var checkMap: Map[Int, (String, String)] = Map()
        var checkSQLSELECT = "SELECT "

        for (i <- tpAr.indices) {
          var a = tpAr(i).subject
          var b = tpAr(i)._3

          checkSQLSELECT += "tp" + i + ", "

          varAr ++= ArrayBuffer(a, b)
          checkMap += (i -> Tuple2(a, b))

          if (!(tpMap.contains(a))) {
            tpMap += ((a) -> ArrayBuffer(Tuple2(i, "subject")))

          } else {
            var temp = tpMap.get(a).get
            temp += Tuple2(i, "subject")
            tpMap.put(a, temp)
          }
          if (!(tpMap.contains(b))) {
            tpMap += ((b) -> ArrayBuffer(Tuple2(i, "`object`")))

          } else {
            var temp = tpMap.get(b).get
            temp += Tuple2(i, "`object`")
            tpMap.put(b, temp)

          }
        }
        checkSQLSELECT = checkSQLSELECT.stripSuffix(", ")

        var cloneTpAr = tpAr.clone()

        var removedMap: Map[String, ArrayBuffer[(Int, String)]] = Map()

        varAr = varAr.distinct
        var checkSQLWHERE = "WHERE "
        checkMap.foreach { ab =>
          var a = ab._2._1
          var b = ab._2._2

          if (varAr.contains(a)) {

            varAr -= a
            var x = tpMap.get(a).get
            for (k <- x) {
              if (k._1 != ab._1) {
                checkSQLWHERE += "tp" + ab._1 + ".subject = tp" + k._1 + "." + k._2 + " AND "

              }

            }

          }

          if (varAr.contains(b)) {

            varAr -= b
            var y = tpMap.get(b).get

            for (k <- y) {
              if (k._1 != ab._1) {
                checkSQLWHERE += "tp" + ab._1 + ".`object` = tp" + k._1 + "." + k._2 + " AND "

              }
            }

          }

        }

        checkSQLWHERE = checkSQLWHERE.stripSuffix(" AND ")
        v.registerTempTable("t")
        println(checkSQLSELECT + " FROM t " + checkSQLWHERE)
        var out = sqlContext.sql(checkSQLSELECT + " FROM t " + checkSQLWHERE)
        out
      }

    }

    def cardPlusnegativeExamplesLength(tpAr: ArrayBuffer[RDFTriple], support: Double, sc: SparkContext, sqlContext: SQLContext): Double = {
      this.dfTable.registerTempTable("kbTable")

      // var card = cardinality(tpAr, sc, sqlContext)
      var go = false
      var outCount: Double = 0.0
      var tpsString = calcName(tpAr)
      for (i <- 1 until tpAr.length) {
        if ((tpAr(i)._1 == "?a") || (tpAr(i)._3 == "?a")) {
          go = true
        }
      }

      if (dfMap.get(tpsString) == None) {
        return outCount
      }

      if ( go ) {

        var card = dfMap.get(tpsString).get

        card.registerTempTable("cardTable")

        var h = sqlContext.sql("SELECT DISTINCT tp0.subject AS sub FROM cardTable")

        var out: DataFrame = null

        if (tpAr.length > 2) {

          out = negatveExampleBuilder(h, tpAr, sc, sqlContext)
        } else {
          var abString = ""
          if (tpAr(1)._1 == "?a") {
            abString = "subject"
          } else {
            abString = "`object`"
          }

          var o = sqlContext.sql("SELECT rdf AS tp0 FROM kbTable WHERE rdf.predicate='" + (tpAr(1)).predicate + "'")
          o.registerTempTable("twoLengthT")
          h.registerTempTable("subjects")
          out = sqlContext.sql("SELECT twoLengthT.tp0 FROM twoLengthT JOIN subjects ON twoLengthT.tp0." + abString + "=subjects.sub")

        }
        outCount = out.count()
      }
      outCount

    }

    def negatveExampleBuilder(subjects: DataFrame, wholeAr: ArrayBuffer[RDFTriple], sc: SparkContext, sqlContext: SQLContext): DataFrame = {
      val DF = this.dfTable
      var tpMap: Map[String, ArrayBuffer[(Int, String)]] = Map()
      DF.registerTempTable("table")
      var wholeTPARBackup = wholeAr.clone()
      wholeAr.remove(0)

      var complete = sqlContext.sql("SELECT rdf AS tp" + 0 + " FROM table WHERE rdf.predicate = '" + (wholeAr(0)).predicate + "'")

      for (i <- 1 until wholeAr.length) {
        var w = sqlContext.sql("SELECT rdf AS tp" + i + " FROM table WHERE rdf.predicate = '" + (wholeAr(i)).predicate + "'")
        w.registerTempTable("newColumn")

        complete.registerTempTable("previousTable")
        complete = sqlContext.sql("SELECT * FROM previousTable JOIN newColumn")
      }

      var varAr: ArrayBuffer[String] = new ArrayBuffer
      var checkMap: Map[Int, (String, String)] = Map()
      var checkSQLSELECT = "SELECT "

      var abString = ("", "")

      for (i <- wholeAr.indices) {
        var a = wholeAr(i).subject
        var b = wholeAr(i)._3

        if ((abString == ("", "")) && (a == "?a")) {
          abString = ("subject", "tp" + i)
        }

        if ((abString == ("", "")) && (b == "?a")) {
          abString = ("`object`", "tp" + i)
        }

        checkSQLSELECT += "tp" + i + ", "

        varAr ++= ArrayBuffer(a, b)
        checkMap += (i -> Tuple2(a, b))

        if (!(tpMap.contains(a))) {
          tpMap += ((a) -> ArrayBuffer(Tuple2(i, "subject")))

        } else {
          var temp = tpMap.get(a).get
          temp += Tuple2(i, "subject")
          tpMap.put(a, temp)
        }
        if (!(tpMap.contains(b))) {
          tpMap += ((b) -> ArrayBuffer(Tuple2(i, "`object`")))

        } else {
          var temp = tpMap.get(b).get
          temp += Tuple2(i, "`object`")
          tpMap.put(b, temp)

        }
      }
      checkSQLSELECT = checkSQLSELECT.stripSuffix(", ")

      var cloneTpAr = wholeAr.clone()

      var removedMap: Map[String, ArrayBuffer[(Int, String)]] = Map()

      varAr = varAr.distinct
      var checkSQLWHERE = "WHERE "
      checkMap.foreach { ab =>
        var a = ab._2._1
        var b = ab._2._2

        if (varAr.contains(a)) {

          varAr -= a
          var x = tpMap.get(a).get
          for (k <- x) {
            if (k._1 != ab._1) {
              checkSQLWHERE += "tp" + ab._1 + ".subject = tp" + k._1 + "." + k._2 + " AND "

            }

          }

        }

        if (varAr.contains(b)) {

          varAr -= b
          var y = tpMap.get(b).get

          for (k <- y) {
            if (k._1 != ab._1) {
              checkSQLWHERE += "tp" + ab._1 + ".`object` = tp" + k._1 + "." + k._2 + " AND "

            }
          }

        }

      }

      checkSQLWHERE = checkSQLWHERE.stripSuffix(" AND ")

      complete.registerTempTable("t")
      var last = sqlContext.sql(checkSQLSELECT + " FROM t " + checkSQLWHERE)
      last.registerTempTable("lastTable")

      subjects.registerTempTable("keyTable")

      var out = sqlContext.sql(checkSQLSELECT + " FROM lastTable JOIN keyTable ON lastTable." + abString._2 + "." + abString._1 + "=keyTable.sub")

      out

    }

    // TODO: solve with DataFrames
    def cardPlusnegativeExamplesLength(triplesCard: ArrayBuffer[RDFTriple], sc: SparkContext): Double = {

      val k = this.kbGraph

      /**
       * in every RDD is an Array with facts that are corresponding to the relations in the rule,
       * the index of triplesCard correspond with the index of arbuf
       *
       */
      var arbuf = new ArrayBuffer[RDD[RDFTriple]]

      var mapList = new ArrayBuffer[Map[String, String]]

      for (i <- triplesCard) {

        var z = i.predicate
        var x = k.find(None, Some(z), None)
        arbuf += x

      }

      /** initializing maplist with head of the rule */
      for (ii <- arbuf(0).collect()) {
        mapList += Map(triplesCard(0).subject -> ii._1, triplesCard(0).`object` -> ii._3)

      }

      var temp = mapList.clone()

      for (tripleCount <- 1 until triplesCard.length) {

        val rdd1 = sc.parallelize(mapList)
        val rdd2 = arbuf(tripleCount)
        val comb = rdd1.cartesian(rdd2) // cartesian() to get every possible combination

        /**
         * the combinations._1 is always the ArrayBuffer with the maps,
         * it represents all correctly mapped placeholders to facts to this point.
         * combinations._2 is the current part of the rule we are mapping
         *
         */
        val combinations = comb.distinct.collect()

        mapList = new ArrayBuffer[Map[String, String]]

        for (i <- combinations) {
          var ltrip = i._2
          var elem1 = ltrip._1 // subject from combination
          var elem2 = ltrip._3
          var trip1 = triplesCard(tripleCount)._1 // subject from Rule
          var trip2 = triplesCard(tripleCount)._3

          /** checking map for placeholder for the subject */
          if (!(i._1.contains(trip1))) {
            i._1 += (trip1 -> elem1)
          }

          /** checking map for placeholder for the object */
          if (!(i._1.contains(trip2))) {
            i._1 += (trip2 -> elem2)
          }

          if ((i._1.get(trip1) == Some(elem1)) && (i._1.get(trip2) == Some(elem2))) {
            mapList += i._1
          }

        }

      }
      var rightOnes = sc.parallelize(mapList).map(y => y.get(triplesCard(0).subject).get).distinct.collect

      var as = sc.parallelize(temp).map {
        x =>
          (x.get(triplesCard(0).subject).get, 1)

      }.reduceByKey(_ + _).collect

      var out: Double = 0.0
      for (i <- as) {
        if (rightOnes.contains(i._1)) {
          out += (i._2 - 1)
        }
      }
      ((mapList.length) + out)
    }

    def addDanglingAtom(c: Int, id: Int, minHC: Double, rule: RuleContainer, sc: SparkContext, sqlContext: SQLContext): DataFrame =
      {
        val tpAr = rule.getRule()
        var RXY: ArrayBuffer[(String, String)] = new ArrayBuffer

        val notC = rule.notClosed()

        val variables = rule.getVariableList()
        val freshVar = "?" + (rule.getHighestVariable() + 1).toChar.toString

        if (notC.isEmpty) {
          for (v <- variables) {
            RXY ++= ArrayBuffer(Tuple2(v, freshVar), Tuple2(freshVar, v))

          }
        } else {
          for (nc <- notC.get) {
            RXY ++= ArrayBuffer(Tuple2(nc, freshVar), Tuple2(freshVar, nc))
          }
        }

        var x = this.countProjectionQueriesDF(c, id, "OD", minHC, tpAr, RXY, sc, sqlContext)

        x
      }

    def addClosingAtom(c: Int, id: Int, minHC: Double, rule: RuleContainer, sc: SparkContext, sqlContext: SQLContext): DataFrame =
      {
        val tpAr = rule.getRule()
        var RXY: ArrayBuffer[(String, String)] = new ArrayBuffer

        val notC = rule.notClosed()

        val variables = rule.getVariableList()

        if (notC.isEmpty) {

          for (v <- variables) {
            for (w <- variables) {
              if (!(v == w)) {
                RXY += Tuple2(v, w)
              }
            }

          }
        } else {
          var notCVars = notC.get

          if (notCVars.length == 1) {
            for (v <- variables) {
              RXY ++= ArrayBuffer(Tuple2(notCVars(0), v), Tuple2(v, notCVars(0)))
            }
          } else {
            for (a <- notCVars) {
              for (b <- notCVars) {
                if (!(a == b)) {
                  RXY += Tuple2(a, b)
                }
              }
            }
          }

        }
        var x = this.countProjectionQueriesDF(c, id, "OC", minHC, tpAr, RXY, sc, sqlContext)

        x
      }

  }
}
