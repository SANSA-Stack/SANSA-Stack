package net.sansa_stack.query.flink.sparqlify

import java.util.function.UnaryOperator

import org.aksw.sparqlify.core.TypeToken
import org.aksw.sparqlify.core.algorithms.DatatypeToString
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

object DatatypeToStringFlink {
  private val logger = LoggerFactory.getLogger(classOf[DatatypeToStringFlink])
}

class DatatypeToStringFlink() // "bpchar", "BPCHAR");
  extends DatatypeToString { // TODO: Use the datatype system map for reverse mapping
  protected val nameToPostgres = HashMap(
    "boolean" -> "boolean",
    "float" -> "double precision",
    "double" -> "double precision",
    "integer" -> "integer",
    "string" -> "varchar",
    "geometry" -> "geometry",
    "geography" -> "geography",
    "int" -> "integer",
    "long" -> "bigint",
    // "d"-> "datetime",;
    "datetime" -> "date",
    "dateTime" -> "date",
    // bigint "geography"-> "geography",;
    "timestamp" -> "timestamp",
    // FIXME Not sure if we really have to may every type explicitely here
    // I guess this should be inferred from the config
    "int4" -> "int4",
    "text" -> "string",
    "VARCHAR" -> "VARCHAR",
    "DOUBLE" -> "DOUBLE",
    "INTEGER" -> "INTEGER",
    "BIGINT" -> "BIGINT",
    "REAL" -> "REAL",
    "TIMESTAMP" -> "TIMESTAMP",
    "DATE" -> "DATE",
    "BOOLEAN" -> "BOOLEAN",
    "VARBINARY" -> "VARBINARY",
    "CHAR" -> "CHAR")

  class AsJavaUnaryOperator[T](sf: scala.Function1[T, T]) extends java.util.function.UnaryOperator[T] {
    def apply(x1: T): T = sf.apply(x1)
  }

  /**
   * Performs a type cast
   */
  override def asString(datatype: TypeToken): UnaryOperator[String] = {
    /**
     * if(datatype.getName().equals("geography")) {
     * return new Factory1<String>() {
     *
     * @Override
     * public String create(String a) {
     * return "ST_AsText(" + a +")";
     * }
     *
     * };
     * }
     */
    var result = nameToPostgres.get(datatype.getName.toLowerCase)
      .orElse(nameToPostgres.get(datatype.getName))
      .getOrElse({
        // System.err.println("WARNING: Datatype not checked for db support");
        DatatypeToStringFlink.logger.trace("WARNING: Datatype not checked for db support")
        // throw new RuntimeException("No string representation for " + datatype.getName());
        datatype.getName
      })
    // return result;
    System.out.println("CAST TO " + result)
    new AsJavaUnaryOperator[String]((a: String) => "CAST(" + a + " AS " + result + ")")
  }
}

