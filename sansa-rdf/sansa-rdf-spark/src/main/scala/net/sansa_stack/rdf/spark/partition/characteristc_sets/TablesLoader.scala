package net.sansa_stack.rdf.spark.partition.characteristc_sets

import java.net.URI

import scala.util.{Failure, Success, Try}

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import net.sansa_stack.rdf.spark.io.index.TriplesIndexer
import org.apache.spark.sql.functions.{col, udf}

/**
 * Create tables and load RDF data into different logical partitioning schemes:
 *
 *  - TT: triples table, i.e. a single table with columns for subject, predicate and object
 *  - VP: vertical partitioning, i.e. one table per property
 *  - WPT: wide property table, i.e. one column for the subject, one column per property with objects being the value
 *  - IWPT: inverse wide property table, i.e. one column for the object, one column per property with subjects being the value
 *
 * @author Lorenz Buehmann
 */
class TablesLoader(spark: SparkSession,
                   database: String = "sansa") {

  val COL_SUBJECT = "s"
  val COL_PREDICATE = "p"
  val COL_OBJECT = "o"
  val COL_DATATYPE = "dt"

  val TRIPLETABLE_NAME = "triples"
  val TRIPLETABLE_EXT_NAME = s"${TRIPLETABLE_NAME}_extended"

  val DATABASE_NAME = "sansa"

  spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
  spark.sql(s"USE $database")

  val extractNodeTypeUDF = udf((s: String) => s.charAt(0) match {
    case '<' => 0
    case '_' => 1
    case '"' => 2
  })
  val extractDatatypeUDF = udf((o: String) => o.charAt(0) match {
    case '"' =>
      val idx = o.lastIndexOf("^^")
      if (idx > 0) {
        o.substring(idx + 2)
      } else {
        null
      }
    case _ => null
  })
  spark.udf.register("extractNodeType", extractNodeTypeUDF.asNonNullable())
  spark.udf.register("extractDatatype", extractDatatypeUDF)

  /**
   * Loads RDF data into a single table with columns for subject, predicate and object.
   *
   * @param path the path to the RDF data
   */
  def loadTriplesTable(path: String): Unit = {
    val regex = """(\\S+)\\s+(\\S+)\\s+(.+)\\s*\\.\\s*$""".r

    var query =
      s"""
        |CREATE EXTERNAL TABLE IF NOT EXISTS ${TRIPLETABLE_NAME}_ext($COL_SUBJECT STRING, $COL_PREDICATE STRING, $COL_OBJECT STRING)
        |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
        |WITH SERDEPROPERTIES	('input.regex' = '$regex')
        |LOCATION '$path'
        |""".stripMargin
    spark.sql(query)

    query =
      s"""
        |CREATE TABLE  IF NOT EXISTS  $TRIPLETABLE_NAME($COL_SUBJECT STRING, $COL_PREDICATE STRING, $COL_OBJECT STRING)
        |STORED AS PARQUET
        |""".stripMargin
    spark.sql(query)

    query =
      s"""
         |INSERT OVERWRITE TABLE $TRIPLETABLE_NAME
         |SELECT DISTINCT $COL_SUBJECT, $COL_PREDICATE, trim($COL_OBJECT)
         |FROM ${TRIPLETABLE_NAME}_ext
         |WHERE $COL_SUBJECT is not null AND $COL_PREDICATE is not null AND $COL_OBJECT is not null
         |""".stripMargin
    spark.sql(query)
  }

  /**
   * Loads RDF data into a single table with columns for subject, predicate and object. Moreover, columns
   * for optional datatype and language tag will be generated.
   *
   * @param path the path to the RDF data
   */
  def loadTriplesTableExtended(path: String): Unit = {
    val regex = """(\\S+)\\s+(\\S+)\\s+(.+)\\s*\\.\\s*$""".r

    var query =
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${TRIPLETABLE_EXT_NAME}_ext($COL_SUBJECT STRING, $COL_PREDICATE STRING, $COL_OBJECT STRING)
         |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
         |WITH SERDEPROPERTIES	('input.regex' = '$regex')
         |LOCATION '$path'
         |""".stripMargin
    spark.sql(query)

    query =
      s"""
         |CREATE TABLE IF NOT EXISTS  $TRIPLETABLE_EXT_NAME($COL_SUBJECT STRING, $COL_PREDICATE STRING, $COL_OBJECT STRING, $COL_DATATYPE STRING)
         |STORED AS PARQUET
         |""".stripMargin
    spark.sql(query)

    query =
      s"""
         |INSERT OVERWRITE TABLE $TRIPLETABLE_EXT_NAME
         |SELECT DISTINCT $COL_SUBJECT, $COL_PREDICATE, trim($COL_OBJECT), extractDatatype(trim($COL_OBJECT))
         |FROM ${TRIPLETABLE_EXT_NAME}_ext
         |WHERE $COL_SUBJECT is not null AND $COL_PREDICATE is not null AND $COL_OBJECT is not null
         |""".stripMargin
    spark.sql(query)

    spark.table(TRIPLETABLE_EXT_NAME).filter(col("dt").isNotNull).show(20, false)
  }

  def loadVPTables(tableNameFn: String => String = uri => "vp_" + escapeSQLName(uri)): Unit = {
    // get all properties from the triples table
    val properties = getProperties()

    // for each property p, create a table with s and o columns
    properties.foreach(p => {
      val tableName = tableNameFn(p)

      spark.sql(s"DROP TABLE IF EXISTS $tableName")

      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS $tableName($COL_SUBJECT STRING, $COL_OBJECT STRING)
           |STORED AS PARQUET
           |""".stripMargin)

      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE $tableName SELECT $COL_SUBJECT, $COL_OBJECT
           |FROM $TRIPLETABLE_NAME
           |WHERE $COL_PREDICATE = '$p'
           |""".stripMargin)
    })
  }

  /**
   * Gets all properties from the triples table.
   *
   * @return the properties
   */
  def getProperties(): Array[String] = {
    spark.sql(s"SELECT DISTINCT $COL_PREDICATE FROM $TRIPLETABLE_NAME").collect().map(_.getString(0)).sorted
  }

  /**
   * Gets all properties from the triples table and for each property also returns whether it has multiple values for
   * a single subject in the current dataset loaded in the triples table.
   *
   * @return the properties and an indicator if it is a multivalue property or just functional
   */
  def getPropertiesWithComplexity(keyColumn: String): Seq[(String, Boolean)] = {
    val mvProperties = spark.sql(
      s"""
         |SELECT DISTINCT $COL_PREDICATE FROM
         |(SELECT $COL_PREDICATE, count(*) as cnt FROM $TRIPLETABLE_NAME GROUP BY $COL_PREDICATE, $keyColumn HAVING cnt > 1)
         |
         |""".stripMargin).collect().map(_.getString(0))

    val svProperties = getProperties().diff(mvProperties)

    (mvProperties.map(p => (p, true)) ++ svProperties.map(p => (p, false))).sortBy(_._1)
  }

  private def loadPTable(keyColumn: String, valueColumn: String): DataFrame = {
    val mergeMapUDF = (data: Seq[Map[String, Array[String]]]) => data.reduce(_ ++ _)
    spark.udf.register("mergeMaps", mergeMapUDF)

    val df = spark.sql(
      s"""
         |SELECT $keyColumn,
         |       mergeMaps(collect_list(po)) AS pos
         |FROM
         |    (SELECT $keyColumn,
         |            map(p, collect_list($valueColumn)) AS po
         |     FROM $TRIPLETABLE_NAME
         |     GROUP BY $keyColumn,
         |              p
         |     )
         |GROUP BY $keyColumn
         |
         |""".stripMargin)

    // get all properties
    val properties = getPropertiesWithComplexity(keyColumn)

    // create a column for each property by looking up the property in the aggregated map
    val selectCols = Seq(keyColumn) ++ properties.map {case (p, mv) => (if (mv) s"element_at(pos, '$p')" else s"if(isnull(element_at(pos, '$p')), NULL, element_at(element_at(pos, '$p'), 1))") + s" as ${escapeSQLName(p)}"}.sorted

    val pt = df.selectExpr(selectCols: _*)

    pt
  }

  def loadWPTable(tableName: String = "wpt"): Unit = {
    val wpt = loadPTable(COL_SUBJECT, COL_OBJECT)
    wpt.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(tableName)
  }

  def loadIWPTable(tableName: String = "iwpt"): Unit = {
    val iwpt = loadPTable(COL_OBJECT, COL_SUBJECT)
    iwpt.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(tableName)
  }

  def escapeSQLName(columnName: String): String = columnName.replaceAll("[<>]", "").trim.replaceAll("[[^\\w]+]", "_")


}

object TablesLoader {
  case class Config(path: String = "",
                    out: String = "",
                    dropDatabase: Boolean = false,
                    databaseName: String = "sansa",
                    schemas: Seq[String] = Seq(),
                    indexed: Boolean = false)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("spark-rdf") {
      head("Spark RDF tables loader", "0.7")

      opt[String]('i', "input").required().valueName("<path>").
        action((x, c) =>
          c.copy(path = x)).text("path to RDF data")
        .validate(x =>
          Try(URI.create(x)) match {
            case Success(uri) => success
            case Failure(s) => failure(s"not a valid input path. Reason: $s")
          })

      opt[String]('o', "out").required().valueName("<path>").
        action((x, c) => c.copy(out = x)).
        text("path to database location")
        .validate(x =>
          Try(URI.create(x)) match {
            case Success(uri) => success
            case Failure(s) => failure(s"not a valid database location. Reason: $s")
          })

      opt[String]( "db").optional().valueName("<dbname>").
        action((x, c) => c.copy(databaseName = x)).
        text("name of database")

      opt[Seq[String]]('s', "schemas").required().valueName("<schema1>,<schema2>,...").action((x, c) =>
        c.copy(schemas = x)).text("schema of partitioning (tt, tt-ext, vp, wpt, iwpt)")

      opt[Unit]("drop-database").action((_, c) =>
        c.copy(dropDatabase = true)).text("drop the database")

      opt[Unit]("indexed").action((_, c) =>
        c.copy(indexed = true)).text("if the values should be indexed")

      help("help").text("prints this usage text")

    }

    parser.parse(args, Config()) match {
      case Some(config) =>

        val spark = SparkSession.builder
          .appName("Spark RDF tables generator")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.warehouse.dir", config.out)
          .config("parquet.enable.dictionary", true)
          .enableHiveSupport()
          .getOrCreate()

        if (config.dropDatabase) spark.sql(s"DROP DATABASE IF EXISTS ${config.databaseName} CASCADE")

        val tl = new TablesLoader(spark, config.databaseName)

        if (config.schemas.contains("tt")) tl.loadTriplesTable(config.path)
        if (config.schemas.contains("tt-ext")) tl.loadTriplesTableExtended(config.path)
        if (config.schemas.contains("vp")) tl.loadVPTables()
        if (config.schemas.contains("wpt")) tl.loadWPTable()
        if (config.schemas.contains("iwpt")) tl.loadIWPTable()

        if (config.indexed) {
          val table = spark.table(tl.TRIPLETABLE_NAME)

          val idx = new StringIndexer()
          val cols = Array("s", "p", "o")
          idx.setInputCols(Array("s", "p", "o"))
          idx.setOutputCols(cols.map(col => s"${col}_idx"))
          val model = idx.fit(table)
          model.write.overwrite().save("/tmp/stringindex.model")

          val idxTable = model.transform(table)
          idxTable.write
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .save(config.out + "/idx")
//
//          val indexer = new TriplesIndexer()
//          val indexedTable = indexer.index(table)
//          indexedTable.show()
//          indexedTable.write
//            .format("parquet")
//            .mode(SaveMode.Overwrite)
//            .save(config.out + "/indexed")

        }

      case None =>
      // arguments are bad, error message will have been displayed
    }



  }
}
