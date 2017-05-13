package net.sansa_stack.inference.utils

import java.util.regex.Pattern

import scala.util.Try

import net.sansa_stack.inference.data.RDFTriple

/**
  * Convert an N-Triples line to an RDFTriple object.
  *
  * @author Lorenz Buehmann
  */
class NTriplesStringToRDFTriple
    extends Function1[String, Option[RDFTriple]]
    with java.io.Serializable
    with Logging {

//  val pattern: Pattern = Pattern.compile("^<([^>]+)>|(?<!<)([^>]+)(?<!>)\\s*<([^>]+)>\\s*<?([^>]+)>?\\s*\\.$")

  val pattern: Pattern = Pattern.compile(
    """|^
       |(<([^>]*)>|(?<!<)([^>]+)(?<!>))
       |\s*
       |<([^>]+)>
       |\s*
       |(<([^>]+)>|(.*))
       |\s*\.$
    """.stripMargin.replaceAll("\n", "").trim)

  override def apply(s: String): Option[RDFTriple] = {
   parseRegexPattern(s)
//    parseRegexSplit(s)
  }

  def parseRegexPattern(s : String): Option[RDFTriple] = Try {
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

      RDFTriple(subject, matcher.group(4), obj)
    } else {
      warn(s"WARN: Illegal N-Triples syntax. Ignoring triple $s")
      throw new Exception(s"WARN: Illegal N-Triples syntax. Ignoring triple $s")
    }

  }.toOption

  def parseRegexSplit(s: String): Option[RDFTriple] = Try {
    val s1 = s.trim
    val split = s1.substring(0, s1.lastIndexOf('.')).split("\\s", 3)
    var obj = split(2).trim
    obj = obj.substring(0, obj.lastIndexOf('.'))
    RDFTriple(split(0), split(1), obj)
  }.toOption

}

object NTriplesStringToRDFTriple {
  def main(args: Array[String]): Unit = {
    val s1 = "<> <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> ."
    val s2 = "_:genid32 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> ."
    val s3 = "<http://example.org/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>  <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> ."
    val s4 = "<http://example.org/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> \"3\"^^<http://ex.org/int> ."

    println(new NTriplesStringToRDFTriple().apply(s1))
    println(new NTriplesStringToRDFTriple().apply(s2))
    println(new NTriplesStringToRDFTriple().apply(s3))
    println(new NTriplesStringToRDFTriple().apply(s4))
  }
}


