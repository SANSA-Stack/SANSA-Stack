package net.sansa_stack.owl.flink.dataset

import com.typesafe.scalalogging.Logger
import org.apache.flink.api.scala.ExecutionEnvironment
import org.semanticweb.owlapi.io.OWLParserException
import org.semanticweb.owlapi.model.{OWLAxiom, OWLRuntimeException}

import net.sansa_stack.owl.common.parsing.ManchesterSyntaxParsing


object ManchesterSyntaxOWLAxiomsDataSetBuilder extends ManchesterSyntaxParsing {
  private val logger = Logger(this.getClass)

  def build(env: ExecutionEnvironment, filePath: String): OWLAxiomsDataSet = {
    val res =
      ManchesterSyntaxOWLExpressionsDataSetBuilder.buildAndGetPrefixes(env, filePath)

    val expressionsDataSet = res._1
    val prefixes = res._2

    val defaultPrefix = prefixes.getOrElse(ManchesterSyntaxParsing._empty,
      ManchesterSyntaxParsing.dummyURI)

    import org.apache.flink.api.scala._
    expressionsDataSet.filter(!_.startsWith("Annotations")).flatMap(frame => {
      try makeAxioms(frame, defaultPrefix)
      catch {
        case exception: OWLParserException =>
          val msg = exception.getMessage
          logger.warn("Parser error for frame\n" + frame + "\n\n" + msg)
          Set.empty[OWLAxiom]
        case exception: OWLRuntimeException =>
          val msg = exception.getMessage
          logger.warn("Parser error for frame\n" + frame + "\n\n" + msg)
          exception.printStackTrace()
          Set.empty[OWLAxiom]
      }
    }).filter(_ != null)
  }
}
