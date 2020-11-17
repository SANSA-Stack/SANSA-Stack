package net.sansa_stack.rdf.spark.partition.characteristc_sets

import org.apache.spark.sql.{SaveMode, SparkSession}

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

  def loadWPTable(): Unit = {
    val mergeMapUDF = (data: Seq[Map[String, Array[String]]]) => data.reduce(_ ++ _)
    spark.udf.register("mergeMaps", mergeMapUDF)

    val df = spark.sql(
    s"""
        |SELECT s,
        |       mergeMaps(collect_list(po)) AS pos
        |FROM
        |    (SELECT s,
        |            map(p, collect_list(o)) AS po
        |     FROM $TRIPLETABLE_NAME
        |        -- (SELECT *
        |        --  FROM triples
        |        --  WHERE s = '<http://dbpedia.org/resource/Alan_Shearer>')
        |     GROUP BY s,
        |              p
        |     ORDER BY s ASC
        |     LIMIT 30)
        |GROUP BY s
        |LIMIT 10
        |
        |""".stripMargin)

    // get all properties
    val properties = getProperties()

    // create a column for each property by looking up the property in the aggregated map
    val selectCols = Seq(COL_SUBJECT) ++ properties.map(p => s"element_at(pos, '$p') as ${escapeSQLName(p)}").sorted

    val wpt = df.selectExpr(selectCols: _*)

    wpt.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("wpt")

  }

  def escapeSQLName(columnName: String): String = columnName.replaceAll("[<>]", "").trim.replaceAll("[[^\\w]+]", "_")

}
