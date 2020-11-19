package net.sansa_stack.rdf.spark.partition.characteristc_sets

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author Lorenz Buehmann
 */
class TablesLoader(spark: SparkSession,
                   database: String = "sansa") {

  val COL_SUBJECT = "s"
  val COL_PREDICATE = "p"
  val COL_OBJECT = "o"

  val TRIPLETABLE_NAME = "triples"

  val DATABASE_NAME = "sansa"

  spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
  spark.sql(s"USE $database")

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
        |CREATE TABLE  IF NOT EXISTS  $TRIPLETABLE_NAME($COL_SUBJECT STRING, $COL_PREDICATE STRING, $COL_OBJECT STRING) STORED AS PARQUET
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

    query = s"SELECT * FROM $TRIPLETABLE_NAME"
    val triplesCnt = spark.sql(query).count()
    println(triplesCnt)
  }

  def loadVPTables(): Unit = {
    val properties = getProperties()

    properties.foreach(p => {
      spark.sql(s"DROP TABLE IF EXISTS vp_${escapeSQLName(p)}")

      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS vp_${escapeSQLName(p)}($COL_SUBJECT STRING, $COL_OBJECT STRING) STORED
           |AS PARQUET
           |""".stripMargin)

      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE vp_${escapeSQLName(p)} SELECT $COL_SUBJECT, $COL_OBJECT
           |FROM $TRIPLETABLE_NAME
           |WHERE $COL_PREDICATE = '$p'
           |""".stripMargin)

      val triplesCnt = spark.sql(s"SELECT * FROM vp_${escapeSQLName(p)}").count()
      println(p + ":" + triplesCnt)
    })
  }

  def getProperties(): Array[String] = {
    spark.sql(s"SELECT DISTINCT $COL_PREDICATE FROM $TRIPLETABLE_NAME").collect().map(_.getString(0))
  }

  def getPropertiesWithComplexity(keyColumn: String): Seq[(String, Boolean)] = {
    val mvProperties = spark.sql(
      s"""
         |SELECT DISTINCT $COL_PREDICATE FROM
         |(SELECT $COL_PREDICATE, count(*) as cnt FROM $TRIPLETABLE_NAME GROUP BY $COL_PREDICATE, $keyColumn HAVING cnt > 1)
         |
         |""".stripMargin).collect().map(_.getString(0))

    val svProperties = getProperties().diff(mvProperties)

    mvProperties.map(p => (p, true)) ++ svProperties.map(p => (p, false))
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

  def loadWPTable(): Unit = {
    val wpt = loadPTable(COL_SUBJECT, COL_OBJECT)
    wpt.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("wpt")
  }

  def loadIWPTable(): Unit = {
    val iwpt = loadPTable(COL_OBJECT, COL_SUBJECT)
    iwpt.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("iwpt")
  }

  def escapeSQLName(columnName: String): String = columnName.replaceAll("[<>]", "").trim.replaceAll("[[^\\w]+]", "_")

}
