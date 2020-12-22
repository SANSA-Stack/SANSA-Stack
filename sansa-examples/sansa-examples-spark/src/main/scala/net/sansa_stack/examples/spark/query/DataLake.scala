package net.sansa_stack.examples.spark.query

import net.sansa_stack.query.spark.datalake.DataLakeEngine
import org.apache.spark.sql.SparkSession

/**
 * Run SPARQL queries over Spark using Data Lake approach.
 */
object DataLake {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.queryFile, config.mappingsFile, config.configFile)
      case None =>
        println(parser.usage)
    }
  }

  def run(queryFile: String, mappingsFile: String, configFile: String): Unit = {

    println("======================================")
    println("|   DataLake (CSV) example           |")
    println("======================================")

    val spark = SparkSession.builder
      .appName(s"DataLake (CSV) example")
      .getOrCreate()

    // val result = spark.sparqlDL(queryFile, mappingsFile, configFile)
    val result = DataLakeEngine.run(queryFile, mappingsFile, configFile, spark)
    result.show()

    spark.stop

  }

  case class Config(
    queryFile: String = getClass.getResource("/datalake/queries/Q1.sparql").getPath,
    mappingsFile: String = getClass.getResource("/datalake/config").getPath,
    configFile: String = getClass.getResource("/datalake/mappings.ttl").getPath)

  val parser = new scopt.OptionParser[Config]("Sparqlify example") {

    head(" DataLake (CSV) example")

    opt[String]('f', "queryFile").valueName("<queryFile>").
      action((x, c) => c.copy(queryFile = x)).
      text("a file containing SPARQL queries or a single query, default: /queries/Q1.sparql")

    opt[String]('m', "mappingsFile").valueName("<mappingsFile>").
      action((x, c) => c.copy(mappingsFile = x)).
      text("the mappings to the target sources, default: /config_csv-only")

    opt[String]('c', "configFile").optional().valueName("<configFile>").
      action((x, c) => c.copy(configFile = x)).
      text("configuration file for different data sources, default: /mappings_csv-only.ttl")

    help("help").text("prints this usage text")
  }

}

