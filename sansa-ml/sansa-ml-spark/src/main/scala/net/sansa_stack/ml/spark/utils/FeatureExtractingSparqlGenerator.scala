package net.sansa_stack.ml.spark.utils

import net.sansa_stack.ml.spark.utils.SPARQLQuery
import net.sansa_stack.ml.spark.utils.ConfigResolver
import org.apache.jena.riot.{Lang, RDFLanguages}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.functions._

import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

import scala.collection.JavaConverters._

object FeatureExtractingSparqlGenerator {

  /**
   * create on string level the seed fetching query
   *
   * @param seedVarName projection var name for seed element
   * @param seedWhereClause where clause how seed can be fetched
   * @param sortedByLinks boolean value if seeds should be ordered by outgoing links in desc order or fifo seeds
   * @return string representing the seed fetching sparql query
   */
  def createSeedFetchingSparql(seedVarName: String, seedWhereClause: String, sortedByLinks: Boolean): String = {
    val seedFetchingSparql = sortedByLinks match {
      case true => f"SELECT DISTINCT $seedVarName \nWHERE { $seedWhereClause \n\tOptional { $seedVarName ?p ?o. } } \ngroup by $seedVarName ORDER BY DESC ( count(?p) ) "
      case false => f"SELECT DISTINCT $seedVarName \n WHERE { $seedWhereClause} }"
    }
    println(f"the generated seed fetching sparql is:\n$seedFetchingSparql")
    seedFetchingSparql
  }

  /**
   * creates dataframe for traversing over join
   *
   * @param df dataframe representing entire graph
   * @return dataframes for traversing up (which is same as df and down which is up flipped and added the traverse direction column)
   */
  def createDataframesToTraverse(df: DataFrame): (DataFrame, DataFrame) = {
    df.toDF(Seq("s", "p", "o"): _*)
    // df.printSchema() TODO be aware that we are operatingsometimes on string sometimes on apache jena node level

    // down
    val down: DataFrame = df.withColumn("dir", typedLit("down"))
    // up
    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node)])
    val isNotLiteral = udf((n: String) => {
      !n.startsWith("\"")
      // !n.asInstanceOf[Node].isLiteral()
    })
    val up: DataFrame = df
      .toDF(Seq("o", "p", "s"): _*)
      .withColumn("dir", typedLit("up"))
      .where(isNotLiteral(col("s"))) // TODO would be good to operate here on node and not on string level

    (up, down)
  }

  /**
   * traverses a tree by joining dataframes of current paths and traversabel hops
   * @param paths current paths initially started at seeds
   * @param traverseDf the dataframe giving traversal opportunities
   * @param iterationLimit how deep to traverse or how often join showld be performed max
   * @param traverseDirection direction whether up or down
   * @return the traversed dataframe with current paths after traverse up, and paths ending with literals after traverse down
   */
  def traverse(paths: DataFrame, traverseDf: DataFrame, iterationLimit: Int, traverseDirection: String, numberRandomWalks: Int = 0): DataFrame = {

    val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    var dataFramesWithOpenEnd: DataFrame = paths
    var dataframeWithLiteralEnd: DataFrame = spark.emptyDataFrame
    var currentPaths = paths

    breakable {
      for (iteration <- 0  to (iterationLimit-1)) {
        // set iterators
        val columnName = f"n_$iteration"
        val iterationPlusOne: Int = iteration + 1
        val columnNamePlusOne = f"n_$iterationPlusOne"

        // paths to merge
        val left: DataFrame = currentPaths
        // here we partially similate random walt behavior
        val right: DataFrame = numberRandomWalks match {
            case 0 => traverseDf.toDF(Seq(f"n_$iteration", f"p_$iteration", f"n_$iterationPlusOne", f"dir_$iteration"): _*)
            case _ => traverseDf.toDF(Seq(f"n_$iteration", f"p_$iteration", f"n_$iterationPlusOne", f"dir_$iteration"): _*).sample(true, 2D*numberRandomWalks/traverseDf.count()).limit(numberRandomWalks)
        }

        // this joines the next hop
        // println(s"joinedPaths dataframe: $iteration")
        val joinedPaths = left.join(right, columnName)

        // current paths are the ones we want to follow in next iteration. so it is reasonable if   TODO better literal identification
        // they end with not literal.
        // see ! excamation mark in where statement
        // println(s"current paths dataframe: $iteration")
        currentPaths = joinedPaths.where(!col(columnNamePlusOne).startsWith("\""))
        // final paths are paths which end with literal
        // this can only happen when traversing down
        val finalPaths = joinedPaths.where(col(columnNamePlusOne).startsWith("\""))

        // filter out cyclic paths from currentPaths
        val noCycle = udf((row: Row) => {
          val l = row.toSeq.toList
            .filter(_!=None)
            .filter(_!=null)
          val lFromSet = l.toSet
          l.length == lFromSet.size
        })
        val nNamedColumns = currentPaths.columns.filter(_.startsWith("n_")).toList
        // println(nNamedColumns)
        // currentPaths.where(!noCycle(struct(nNamedColumns.map(col): _*))).show(false)
        currentPaths = currentPaths.where(noCycle(struct(nNamedColumns.map(col): _*)))

        // println(s"$iteration filtered current paths")

        // append the paths we finally travered until literal is reached
        // in up this will not happen
        if (finalPaths.count() > 0) {
          val recentColumns: Seq[String] = dataframeWithLiteralEnd.columns.toSeq
          val noneColumnsToAdd: Seq[String] = finalPaths.columns.toSeq.toSet.diff(recentColumns.toSet).toSeq
          var df1 = dataframeWithLiteralEnd
          for (c <- noneColumnsToAdd) df1 = df1.withColumn(c, lit(null: String))
          val  df2 = finalPaths
          val df3 = df2.union(df1.select(df2.columns.map(col(_)): _*))
          dataframeWithLiteralEnd = df3
          dataframeWithLiteralEnd = dataframeWithLiteralEnd.union(finalPaths)
        }

        if (currentPaths.count() == 0) {
          // println(f"no remaining paths are available so: $traverse_direction is done")
          break
        }

        // if we traverse up we change column names s.t. last elment added is alway in column n0 s.t. join in traverse down is easier
        if (traverseDirection == "up") {
          val tmpPaths: DataFrame = currentPaths
          val tmpColumns = tmpPaths.columns.toSeq
          val newTmpColumns: Seq[String] = tmpColumns.map(c => {
            val currentNumber: Int = c.split("_").last.toInt
            val currentChars: String = c.split("_")(0)
            val newNumber = (currentNumber - iterationPlusOne).toString
            val newColumnName = currentChars + "_" + newNumber
            newColumnName
          })
          val recentColumns: Seq[String] = dataFramesWithOpenEnd.columns.toSeq
          val noneColumnsToAdd = newTmpColumns.toSet.diff(recentColumns.toSet).toSeq
          var df1 = dataFramesWithOpenEnd
          for (c <- noneColumnsToAdd) df1 = df1.withColumn(c, lit(null: String))
          val  df2 = tmpPaths.toDF(newTmpColumns: _*)
          val df3 = df2.union(df1.select(df2.columns.map(col(_)): _*))
          dataFramesWithOpenEnd = df3
        }
      }
    }
    val dfLit = numberRandomWalks match {
      case 0 => dataframeWithLiteralEnd
      case _ => dataframeWithLiteralEnd.sample(true, 2D*numberRandomWalks/dataframeWithLiteralEnd.count()).limit(numberRandomWalks)
    }

    val returnDataframe = traverseDirection match {
      case "up" => dataFramesWithOpenEnd
      case "down" => dfLit
    }
    returnDataframe
  }

  /**
   * creates a string corresponding to an OPTIONAL block for where part in resulting sparql
   *
   * @param row row from dataframe created by traversing all paths
   * @param seedVarName name of seed projection var
   * @return string representing OPTIONAL block
   */
  def rowToQuery(row: Row, seedVarName: String): (String, String) = {

    val nonNullRow: List[String] = row.toSeq.toList.filter(_!=None).filter(_!=null).asInstanceOf[List[String]]

    val lenRow: Int = nonNullRow.size
    val numberQueryLines: Int = (lenRow-1)/3

    var varNames = ListBuffer(seedVarName)

    var projectionVar: String = ""

    var queryStr = "\tOPTIONAL {\n"

    for (queryLineNumber <- 0 to (numberQueryLines-1)) {
      val leftN = nonNullRow(queryLineNumber * 3)
      val p = nonNullRow((queryLineNumber * 3) + 1)
      val direction = nonNullRow((queryLineNumber * 3) + 2)
      val rightN = nonNullRow((queryLineNumber * 3) + 3)

      var firstVarName = varNames.last
      var secondVarName = firstVarName + f"__$direction" + "_" + p.toString.split("/").last.replace("#", "_")
      varNames.append(secondVarName)
      val query_line: String = direction match {
        case "down" => f"$firstVarName <$p> $secondVarName ."
        case "up" => f"$secondVarName <$p> $firstVarName ."
      }
      queryStr = queryStr + f"\t\t$query_line\n"
      projectionVar = secondVarName
    }
    queryStr = queryStr + "\t}"

    (queryStr, projectionVar)
  }

  /**
   * this function creates the sparql and a list of corresponding porjection variables
   *
   * the function operates on dataframe level and first fetches the seeds
   * then seeds are cutof to the desired number or ration to be considered
   * from seeds we traverse up in the graph
   * traverse down
   * create for each traversed path a query line
   * take unique query lines
   * create sparql query
   *
   * @param df dataframe of true columns of type string representing triples  s p o
   * @param seedVarName how the seeds should be named and with beginnig questionmark as needed for projection variable
   * @param seedWhereClause a string representing the where part of a sparql query specifying how to reach seeds
   * @param maxUp integer for limiting number of traversal up steps
   * @param maxDown integer for limiting traverse down steps
   * @param numberSeeds number of seeds to consider
   * @param ratioNumberSeeds number of seeds specified by ratio
   * @return string of resulting sparql and list of string for each projection variable which later can be used for dataframe column naming
   */
  def autoPrepo(
     df: DataFrame,
     seedVarName: String,
     seedWhereClause: String,
     maxUp: Int,
     maxDown: Int,
     numberSeeds: Int = 0,
     ratioNumberSeeds: Double = 1.0,
     numberRandomWalks: Int = 0,
     hardCodedSeeds: List[String] = List.empty
               ): (String, List[String]) = {

    val spark = SparkSession.builder
      .getOrCreate()
    import spark.implicits._
    implicit val nodeEncoder = Encoders.kryo(classOf[Node])
    implicit val nodeTupleEncoder = Encoders.tuple[Node, Node, Node](nodeEncoder, nodeEncoder, nodeEncoder)

    val ds = df.toDS().cache()

    // create the sparql to reach seeds and maybe sort them by ths sparql as well
    val seedFetchingSparql: String = createSeedFetchingSparql(seedVarName, seedWhereClause, sortedByLinks = true)

    // query for seeds and list those
    val querytransformer1: SPARQLQuery = SPARQLQuery(seedFetchingSparql)
    val seedsDf: DataFrame = querytransformer1.transform(ds).cache()
    seedsDf.show(false)
    seedsDf.as[Node].map(_.toString()).toDF().show(false)
    val seeds: List[Node] = seedsDf.as[Node].rdd.collect().toList
    seeds.foreach(println(_))
    println(f"the fetched seeds are:\n$seeds\n")
    val numberSeeds: Int = seeds.length

    // calculate cutoff
    var cutoff: Int = numberSeeds
    cutoff = if (numberSeeds >= 0) numberSeeds else cutoff
    cutoff = math.rint(numberSeeds * ratioNumberSeeds).toInt
    val usedSeeds: List[Node] = seeds.take(cutoff)
    val usedSeedsAsString = usedSeeds.map(_.toString)

    // val usedSeedsAsString: List[String] = hardCodedSeeds // List("http://dig.isi.edu/John_jr", "http://dig.isi.edu/Mary", "http://dig.isi.edu/John") // usedSeeds.map(_.toString)

    // create dataframes for traversal (up and down)
    val (up: DataFrame, down: DataFrame) = createDataframesToTraverse(df)

    // seeds in dataframe asstarting paths
    println(s"we start initially with following seeds:\n${usedSeedsAsString.mkString("\n")}")
    println("initial paths, so seeds are:")
    var paths: DataFrame = usedSeedsAsString.toDF("n_0").cache() // seedsDf.map(_.toString).limit(cutoff).toDF("n0")
    paths.show(false)
    // traverse up
    println("traverse up")
    paths = traverse(paths, up, iterationLimit = maxUp, traverseDirection = "up", numberRandomWalks = numberRandomWalks).cache()
    paths.show(false)
    // traverse down
    println("traverse down")
    paths = traverse(paths, down, iterationLimit = maxDown, traverseDirection = "down", numberRandomWalks = numberRandomWalks).cache()
    paths.show(false)
    // all gathered paths
    println("gathered paths")
    val columns = paths.columns.toList

    val newColumnsOrder: Seq[String] = columns
      .map(_.split("_").last.toInt)
      .distinct
      .sorted
      .dropRight(1)
      .flatMap(i => (f"n_$i p_$i dir_$i n_${i + 1}").split(" "))
      .distinct

    paths = paths.select(newColumnsOrder.map(col(_)): _*).cache()

    val results = paths.rdd.map(rowToQuery(_, seedVarName)).cache()

    val queryLines: List[String] = results.map(_._1.toString).collect().toList.distinct.sortBy(_.size)
    val projectionVars: List[String] = results.map(_._2.toString).collect().toList.distinct.sortBy(_.size)

    val projection_vars_string = projectionVars.mkString(" ")
    val all_optional_query_blocks_str = queryLines.mkString("\n")
    val total_query = f"SELECT $seedVarName $projection_vars_string\n\nWHERE {\n\t${seedWhereClause}\n\n${all_optional_query_blocks_str}}}"

    (total_query, projectionVars)
  }

  /**
   * the main function call the entire process
   *
   * all configuration have to be done in a config file. this allows easier interaction as soon as a standalone jar has been created.
   *
   * @param args path to the typesafe conf file
   */
  def main(args: Array[String]): Unit = {

    val configFilePath = args(0)
    val config = new ConfigResolver(configFilePath).getConfig()

    println(config)

    val inputFilePath: String = config.getString("inputFilePath") // "/Users/carstendraschner/Downloads/test.ttl"
    val outputFilePath: String = config.getString("outputFilePath") // "/Users/carstendraschner/Downloads/test.ttl"

    val seedVarName = config.getString("seedVarName") // "?seed"
    val whereClauseForSeed = config.getString("whereClauseForSeed") // "?seed a <http://dig.isi.edu/Person>"

    val maxUp: Int = config.getInt("maxUp") // 5
    val maxDown: Int = config.getInt("maxDown") // 5

    val seedNumber: Int = config.getInt("seedNumber") // 0
    val seedNumberAsRatio: Double = config.getDouble("seedNumberAsRatio") // 1.0

    val numberRandomWalks: Int = config.getInt("numberRandomWalks")

    val hardCodedSeeds: List[String] = config.getStringList("hardCodedSeeds").asScala.toList

    // setup spark session
    val spark = SparkSession.builder
      .appName(s"rdf2feature")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
                      "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify",
                      "net.sansa_stack.query.spark.ontop.KryoRegistratorOntop"))
      .config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node)])

    // get lang from filename
    val lang = RDFLanguages.filenameToLang(inputFilePath)

    // load RDF to Dataframe
    val df: DataFrame = spark.read.rdf(lang)(inputFilePath).cache()

    println("The dataframe looks like this:")
    df.show(false)

    val (totalSparqlQuery: String, var_names: List[String]) = autoPrepo(
      df = df,
      seedVarName = seedVarName,
      seedWhereClause = whereClauseForSeed,
      maxUp = maxUp,
      maxDown = maxDown,
      numberSeeds = seedNumber,
      ratioNumberSeeds = seedNumberAsRatio,
      numberRandomWalks = numberRandomWalks,
      hardCodedSeeds = hardCodedSeeds,
    )

    println(
      f"""
         |The automatic created feature extracting sparql fetched ${var_names.size} projection variables representing literals.
         |the projection variables are:
         |${var_names.map(vn => f"\t$vn").mkString("\n")}
         |\n
         |""".stripMargin)
    println(f"The resulting sparql query is: \n$totalSparqlQuery")

    Files.write(Paths.get(outputFilePath), totalSparqlQuery.getBytes(StandardCharsets.UTF_8))
    println(f"generated sparql has been stored to: $outputFilePath")
  }
}