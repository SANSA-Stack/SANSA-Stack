package net.sansa_stack.rdf.spark.io.rdfxml

import com.fasterxml.jackson.core.JsonParser
import org.apache.jena.riot.RDFParserBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
  * Options for parsing RDF/XML data into Spark SQL rows.
  *
  * Most of these map directly to Jena's internal options, specified in [[RDFParserBuilder]].
  */
private[rdfxml] class RdfXmlOptions(
                                   @transient private val parameters: CaseInsensitiveMap[String],
                                   defaultTimeZoneId: String,
                                   defaultColumnNameOfCorruptRecord: String)
  extends Logging with Serializable {

  def this(
            parameters: Map[String, String],
            defaultTimeZoneId: String,
            defaultColumnNameOfCorruptRecord: String = "") = {
    this(
      CaseInsensitiveMap(parameters),
      defaultTimeZoneId,
      defaultColumnNameOfCorruptRecord)
  }

  def defaultErrorHandler: String = "std"

  val baseURI =
    parameters.get("baseURI").getOrElse(null)
  val errorHandler =
    parameters.get("errorHandler").map(_.toBoolean).getOrElse(false)
  val wholeFile = parameters.get("wholeFile").map(_.toBoolean).getOrElse(false)

  //  // Parse mode flags
  //  if (!ParseModes.isValidMode(parseMode)) {
  //    logWarning(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  //  }
  //
  //  val failFast = ParseModes.isFailFastMode(parseMode)
  //  val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  //  val permissive = ParseModes.isPermissiveMode(parseMode)

  /** Sets config options on a Jena [[org.apache.jena.riot.RDFParserBuilder]]. */
  def setParserOptions(parserBuilder: RDFParserBuilder): Unit = {
    parserBuilder.base(baseURI)
  }
}
