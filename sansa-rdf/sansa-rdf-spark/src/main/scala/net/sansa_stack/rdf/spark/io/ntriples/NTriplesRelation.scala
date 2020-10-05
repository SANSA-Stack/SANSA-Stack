package net.sansa_stack.rdf.spark.io.ntriples

import java.io.ByteArrayInputStream
import java.util.regex.Pattern

import scala.util.{Failure, Success, Try}

import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.io.ntriples.ParseMode._
import net.sansa_stack.rdf.spark.io.ntriples.ParseMode.ParseMode
import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.jena.graph.Node
import org.apache.jena.riot.lang.LangNTriples
import org.apache.jena.riot.system.RiotLib
import org.apache.jena.riot.tokens.{Tokenizer, TokenizerFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * A custom relation that represents N-Triples.
  *
  * @param location
  * @param userSchema
  * @param sqlContext
  * @param mode how to parse each line in the N-Triples file (DEFAULT: [[ParseMode]].`REGEX`)
  */
class NTriplesRelation(location: String, userSchema: StructType, val mode: ParseMode = REGEX)
                      (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with PrunedScan
    with Serializable
    with Logging {

  /**
    * Whether to skip blank lines or throw an exception.
    */
  val skipBlankLines = true

  override def schema: StructType = {
    if (this.userSchema != null) {
      this.userSchema
    }
    else {
      StructType(
        Seq(
          StructField("s", StringType, nullable = true),
          StructField("p", StringType, nullable = true),
          StructField("o", StringType, nullable = true)
        ))
    }
  }

  override def buildScan(): RDD[Row] = {
    val rdd = sqlContext
      .sparkContext
      .textFile(location)

    val rows = mode match {
      case REGEX => rdd.map(line => Row.fromTuple(parseRegexPattern(line)))
      case SPLIT => rdd.map(line => Row.fromSeq(line.split(" ").toList))
      case JENA => rdd.map(parseJena(_).get).map(toRow(_))
    }
    rows
  }

  // scan with column pruning
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    // load the RDD of lines first
    val rdd = sqlContext
      .sparkContext
      .textFile(location)

    // map column names to positions in triple
    implicit val positions = requiredColumns.map(
      {
        case "s" => 1
        case "p" => 2
        case "o" => 3
      }
    )

    // apply different line processing based on the configured parsing mode
    val tuples = mode match {
      case REGEX => rdd.map(line => {
        val tripleOpt = parseRegexPattern(line)
        if(tripleOpt.isDefined) {
          Some(extractFromTriple(tripleOpt.get))
        } else {
          None
        }
      })
      case SPLIT => rdd.map(line => Some(extractFromTriple(parseRegexSplit(line))))
      case JENA => rdd.map(line => Some(extractFromJenaTriple(parseJena(line).get).map(_.toString)))
    }

    val rows = tuples.flatMap(t => {
      if (t.isDefined) {
        Some(Row.fromSeq(t.get))
      } else {
        // TODO error handling
        None
      }
    })

    rows
  }

  private def extractFromTriple(triple: (String, String, String))(implicit positions: Array[Int]): Seq[String] = {
    positions.map({
      case 1 => triple._1
      case 2 => triple._2
      case 3 => triple._3
    }).toSeq
  }

  private def extractFromJenaTriple(triple: org.apache.jena.graph.Triple)(implicit positions: Array[Int]): Seq[Node] = {
    positions.map({
      case 1 => triple.getSubject
      case 2 => triple.getPredicate
      case 3 => triple.getObject
    }).toSeq
  }

  /**
    * Parse with Jena API
    * @param s
    * @return
    */
  private def parseJena(s: String): Try[org.apache.jena.graph.Triple] = {
    // always close the streams
    cleanly(new ByteArrayInputStream(s.getBytes))(_.close()) { is =>
      val profile = RiotLib.dftProfile
      val tokenizer: Tokenizer = TokenizerFactory.makeTokenizerUTF8(is)
      val parser = new LangNTriples(tokenizer, profile, null)
      parser.next()
    }
  }

  // the REGEX pattern for N-Triples
  val pattern: Pattern = Pattern.compile(
    """|^
       |(<([^>]*)>|(?<!<)([^>]+)(?<!>))
       |\s*
       |<([^>]+)>
       |\s*
       |(<([^>]+)>|(.*))
       |\s*[.]\s*(#.*)?$
    """.stripMargin.replaceAll("\n", "").trim)

  /**
    * Parse with REGEX pattern
    * @param s
    * @return
    */
  private def parseRegexPattern(s: String): Option[(String, String, String)] = {
    // skip blank lines
    if (s.trim.isEmpty) {
      None
    } else {

      val matcher = pattern.matcher(s)

//      println(matcher.matches() + "---" + s)

      if (matcher.matches) {
        //            for(i <- 0 to matcher.groupCount())
        //              println(i + ":" + matcher.group(i))

        // parse the subject
        val subject = if (matcher.group(2) == null) { // this means it's a blank node captured in group 1 (or 3)
          matcher.group(1)
        } else { // it is a URI
          matcher.group(2)
        }

        // parse the predicate
        val predicate = matcher.group(4)

        // parse the object
        val obj = if (matcher.group(6) == null) { // this means it is a literal
          matcher.group(7).trim
        } else { // it is a URI
          matcher.group(6)
        }

        Some((subject, predicate, obj))
      } else {
        throw new Exception(s"WARN: Illegal N-Triples syntax. Ignoring triple $s")
      }
    }
  }

  /**
    * Parse with simple split on whitespace characters and replace <, >, and . chars
    * @param s
    * @return
    */
  private def parseRegexSplit(s: String): (String, String, String) = {
    val s1 = s.trim
    val split = s1.substring(0, s1.lastIndexOf('.')).split("\\s", 3)
    var obj = split(2).trim
    obj = obj.substring(0, obj.lastIndexOf('.'))
    (split(0), split(1), obj)
  }

  private def cleanly[A, B](resource: A)(cleanup: A => Unit)(doWork: A => B): Try[B] = {
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
        case e: Exception => log.error(e.getMessage) // should be logged
      }
    }
  }
}
