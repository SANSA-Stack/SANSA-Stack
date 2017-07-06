package net.sansa_stack.inference.spark.data.loader.sql

import java.io.ByteArrayInputStream
import java.util.regex.Pattern

import net.sansa_stack.inference.utils.NTriplesStringToRDFTriple
import org.apache.jena.riot.lang.LangNTriples
import org.apache.jena.riot.system.RiotLib
import org.apache.jena.riot.tokens.{Tokenizer, TokenizerFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.util.{Failure, Success, Try}

/**
  * A custom relation that represents N-Triples.
  *
  * @param location
  * @param userSchema
  * @param sqlContext
  */
class NTriplesRelation(location: String, userSchema: StructType, val mode: String = "regex")
                      (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with PrunedScan
    with Serializable {

  override def schema: StructType = {
    if (this.userSchema != null) {
      this.userSchema
    }
    else {
      StructType(
        Seq(
          StructField("s", StringType, true),
          StructField("p", StringType, true),
          StructField("o", StringType, true)
        ))
    }
  }

  override def buildScan(): RDD[Row] = {
    val rdd = sqlContext
      .sparkContext
      .textFile(location)

    val rows = mode match {
      case "regex" => rdd.map(line => Row.fromTuple(parseRegexPattern(line)))
      case "split" => rdd.map(line => Row.fromSeq(line.split(" ").toList))
      case "jena1" => rdd.map(parseJena1(_).get).map(t => Row.fromSeq(Seq(t.getSubject.toString, t.getPredicate.toString, t.getObject.toString)))
      case "jena2" => rdd.map(line => Row.fromTuple(parseJena2(line)))

    }
    rows
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val rdd = sqlContext
      .sparkContext
      .textFile(location)

    val rows = mode match {
      case "regex" => rdd.map(line => Row.fromTuple(parseRegexPattern(line)))
      case "split" => rdd.map(line => Row.fromSeq(line.split(" ").toList))
      case "jena" => rdd.map(parseJena1(_).get).map(t => Row.fromSeq(Seq(t.getSubject.toString, t.getPredicate.toString, t.getObject.toString)))
      case "jena_alt" => rdd.map(line => Row.fromTuple(parseJena2(line)))

    }

    //    val rows = rdd.map(line => Row.fromTuple(parseRegexPattern(line)))
    //    val rows = rdd.map(line => Row.fromSeq(line.split(" ").toList))
    //    val rows = rdd.flatMap(x => converter.apply(x)).map(t => {
    //      val nodes = requiredColumns.map({
    //        case "s" => t.s
    //        case "p" => t.p
    //        case "o" => t.o
    //      })
    //      Row.fromSeq(nodes)
    //    })

    rows
  }

  def parseJena1(s: String): Try[org.apache.jena.graph.Triple] = {
    cleanly(new ByteArrayInputStream(s.getBytes))(_.close()) { is =>
      val profile = RiotLib.dftProfile
      val tokenizer: Tokenizer = TokenizerFactory.makeTokenizerUTF8(is)
      val parser = new LangNTriples(tokenizer, profile, null)
      parser.next()
    }
  }

  def parseJena2(s: String): (String, String, String) = {
    val converter = new NTriplesStringToRDFTriple()
    val t = converter.apply(s).get
    (t.s, t.p, t.o)
  }

  val pattern: Pattern = Pattern.compile(
    """|^
       |(<([^>]*)>|(?<!<)([^>]+)(?<!>))
       |\s*
       |<([^>]+)>
       |\s*
       |(<([^>]+)>|(.*))
       |\s*\.$
    """.stripMargin.replaceAll("\n", "").trim)

  def cleanly[A, B](resource: A)(cleanup: A => Unit)(doWork: A => B): Try[B] = {
    try {
      Success(doWork(resource))
    } catch {
      case e: Exception => Failure(e)
    }
    finally {
      try {
        if (resource != null) {
          cleanup(resource)
        }
      } catch {
        case e: Exception => println(e) // should be logged
      }
    }
  }

  def parseRegexPattern(s: String): (String, String, String) = {
    val matcher = pattern.matcher(s)

    if (matcher.matches) {
      //      for(i <- 0 to matcher.groupCount())
      //        println(i + ":" + matcher.group(i))

      val subject = if (matcher.group(2) == null) {
        matcher.group(1)
      } else {
        matcher.group(2)
      }

      val obj = if (matcher.group(6) == null) {
        matcher.group(7).trim
      } else {
        matcher.group(6)
      }

      (subject, matcher.group(4), obj)
    } else {
      throw new Exception(s"WARN: Illegal N-Triples syntax. Ignoring triple $s")
    }

  }

  def parseRegexSplit(s: String): (String, String, String) = {
    val s1 = s.trim
    val split = s1.substring(0, s1.lastIndexOf('.')).split("\\s", 3)
    var obj = split(2).trim
    obj = obj.substring(0, obj.lastIndexOf('.'))
    (split(0), split(1), obj)
  }
}