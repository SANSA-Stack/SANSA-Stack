package net.sansa_stack.owl.spark.rdd

import com.typesafe.scalalogging.Logger
import net.sansa_stack.owl.common.parsing.ManchesterSyntaxParsing
import org.apache.spark.SparkContext
import org.semanticweb.owlapi.io.OWLParserException
import org.semanticweb.owlapi.model.{OWLAxiom, OWLRuntimeException}


object ManchesterSyntaxOWLAxiomsRDDBuilder extends ManchesterSyntaxParsing {
  private val logger = Logger(this.getClass)

  def build(sc: SparkContext, filePath: String): OWLAxiomsRDD = {
    val res = ManchesterSyntaxOWLExpressionsRDDBuilder.buildAndGetPrefixes(sc, filePath)

    val expressionsRDD: OWLExpressionsRDD = res._1
    val prefixes: Map[String, String] = res._2
    val dummyURI = "http://sansa-stack.net/dummy"
    val defaultPrefix = prefixes.get(ManchesterSyntaxParsing._empty).getOrElse(dummyURI)

    expressionsRDD.filter(!_.startsWith("Annotations")).flatMap(frame => {
      try makeAxioms(frame, defaultPrefix)
      catch {
        case exception: OWLParserException => {
          val msg = exception.getMessage
          logger.warn("Parser error for frame\n" + frame + "\n\n" + msg)
//          exception.printStackTrace()
          Set.empty[OWLAxiom]
        }
        case exception: OWLRuntimeException => {
          val msg = exception.getMessage
          logger.warn("Parser error for frame\n" + frame + "\n\n" + msg)
          exception.printStackTrace()
          Set.empty[OWLAxiom]
        }
      }
    })
  }
}
