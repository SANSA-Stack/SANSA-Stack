package net.sansa_stack.rdf.common.partition.utils

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import net.sansa_stack.rdf.common.partition.core.RdfPartitionStateDefault
import org.apache.jena.graph.NodeFactory

/**
 * Some utilities for working with SQL objects.
 *
 * @author Lorenz Buehmann
 */
object SQLUtils {

  import scala.util.matching.Regex
  val qualifiedTableNameDoubleQuotesRegex: Regex = "^(\"(.*)\".)?\"(.*)\"$".r
  val qualifiedTableNameBackticksRegex: Regex = "^(`(.*)`.)?`(.*)`$".r

  def parseTableIdentifier(tableName: String): TableIdentifier = {
    qualifiedTableNameDoubleQuotesRegex.findFirstMatchIn(tableName) match {
      case Some(i) =>
        val tn = i.group(3)
        val dn = Option(i.group(2))
        TableIdentifier(tn, dn)
      case None => TableIdentifier(tableName, None)
    }
  }

  /**
   * Encode the given table name. This avoids file system issues with e.g. URLs being used as directories
   * as it would happen when saving the table to disk.
   * @param name the table name to encode
   * @return the encoded table name
   */
  def encodeTablename(name: String): String = {
    URLEncoder.encode(name, StandardCharsets.UTF_8.toString)
      .toLowerCase
      .replace('%', 'p')
      .replace('.', 'c')
      .replace("-", "dash")
  }

  /**
   * Creates a SQL table name for a partition state.
   *
   * @note The table name might have to be escaped depending on the database. You can use [[org.aksw.commons.sql.codec.api.SqlCodec]] classes
   *       for this.
   *
   * @param p the RDF partition state
   * @return the SQL table name
   */
  def createDefaultTableName(p: RdfPartitionStateDefault): String = {

    // For now let's just use the full predicate as the uri
    // val predPart = pred.substring(pred.lastIndexOf("/") + 1)
    val predPart = p.predicate
    val pn = NodeFactory.createURI(p.predicate)

    val dt = p.datatype
    val dtPart = if (dt != null && dt.nonEmpty) "_" + dt.substring(dt.lastIndexOf("/") + 1) else ""
    val langPart = if (p.langTagPresent) "_lang" else ""

    val sTermTypePart = if (p.subjectType == 0) "sbn" else ""
    val oTermTypePart = if (p.objectType == 0) "obn" else ""

    val tableName = predPart + dtPart + langPart + sTermTypePart + oTermTypePart

    tableName
  }

}

sealed trait IdentifierWithDatabase {
  val identifier: String

  def database: Option[String]

  /*
   * Escapes back-ticks within the identifier name with double-back-ticks.
   */
  private def quoteIdentifier(name: String, quoteChar: Char = '`'): String = name.replace("`", "``")

  def quotedString: String = {
    val replacedId = quoteIdentifier(identifier)
    val replacedDb = database.map(quoteIdentifier(_))

    if (replacedDb.isDefined) s"`${replacedDb.get}`.`$replacedId`" else s"`$replacedId`"
  }

  def quotedString(quoteChar: Char): String = {
    val replacedId = quoteIdentifier(identifier, quoteChar)
    val replacedDb = database.map(quoteIdentifier(_, quoteChar))

    if (replacedDb.isDefined) s"$quoteChar${replacedDb.get}$quoteChar.$quoteChar$replacedId$quoteChar" else s"$quoteChar$replacedId$quoteChar"
  }

  def unquotedString: String = {
    if (database.isDefined) s"${database.get}.$identifier" else identifier
  }

  override def toString: String = quotedString
}

case class TableIdentifier(table: String, database: Option[String])
  extends IdentifierWithDatabase {

  override val identifier: String = table

  def this(table: String) = this(table, None)
}


