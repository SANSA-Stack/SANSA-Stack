package org.dissect.rdf.spark.utils
import scala.io.Source
import java.io._
import org.dissect.rdf.spark.utils._

/**
 * Read nt file data and format to a triple
 */
object NTripleReader extends Logging {
  def fromFile(ntFile: File): NTripleReader = new NTripleReader(ntFile)

  class NTripleReader(ntFile: File) extends Traversable[(String, String, String, String)] {

    override def foreach[U](f: ((String, String, String, String)) => U) {

      var lines: Array[String] = null

      try {
        lines = Source.fromFile(ntFile).getLines().toArray
      } catch {
        case e: java.nio.charset.MalformedInputException => lines = Source.fromFile(ntFile).getLines().toArray
      }

      // Parse the data into triples.
      var elements = new Array[String](3)
      for (line <- 0 until lines.length) {

        // Space in next line needed for line after that. 
        lines(line) = NTriplesParser.tripleEndingPattern().replaceFirstIn(lines(line), " ")
        elements = lines(line).mkString.split(">\\s+") // split on "> "

        if (elements.length > 1) {
          val subj = elements(0).substring(1) // substring() call
          val pred = elements(1).substring(1) // to remove "<"
          var obj = ""
          var lit = ""
          if (elements(2)(0) == '<') {
            obj = elements(2).substring(1) // Lose that <.
          } else {
            lit = elements(2)
          }
          f((subj, pred, obj, lit))
        } else {
          logger.error("Line must be 'nt' format {%d}", line)
        }
      }
    }
  }
}