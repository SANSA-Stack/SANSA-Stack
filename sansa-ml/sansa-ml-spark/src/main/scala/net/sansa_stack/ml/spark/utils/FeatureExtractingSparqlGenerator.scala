package net.sansa_stack.ml.spark.utils

import net.sansa_stack.ml.spark.utils.SPARQLQuery
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object FeatureExtractingSparqlGenerator {

  def createSeedFetchingSparql(seedVarName: String, seedWhereClause: String, sortedByLinks: Boolean): String = {
    val seedFetchingSparql = sortedByLinks match {
      case true => f"SELECT DISTINCT $seedVarName \nWHERE { $seedWhereClause \n\tOptional { $seedVarName ?p ?o. } } \ngroup by $seedVarName ORDER BY DESC ( count(?p) ) "
      case false => f"SELECT DISTINCT $seedVarName \n WHERE { $seedWhereClause} }"
    }
    println(f"the generated seed fetching sparql is:\n$seedFetchingSparql")
    seedFetchingSparql
  }

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

  def traverse(paths: DataFrame, traverseDf: DataFrame, iterationLimit: Int, traverseDirection: String): DataFrame = {

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
        val right: DataFrame = traverseDf.toDF(Seq(f"n_$iteration", f"p_$iteration", f"n_$iterationPlusOne", f"dir_$iteration"): _*)

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
    val returnDataframe = traverseDirection match {
      case "up" => dataFramesWithOpenEnd
      case "down" => dataframeWithLiteralEnd
    }
    returnDataframe
  }

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

  def autoPrepo(df: DataFrame, seedVarName: String, seedWhereClause: String, maxUp: Int, maxDown: Int, numberSeeds: Int = 0, ratioNumberSeeds: Double = 1.0): (String, List[String]) = {

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
    val seeds: List[Node] = seedsDf.as[Node].rdd.collect().toList
    println(f"the fetched seeds are:\n$seeds\n")
    val numberSeeds: Int = seeds.length

    // calculate cutoff
    var cutoff: Int = numberSeeds
    cutoff = if (numberSeeds >= 0) numberSeeds else cutoff
    cutoff = math.rint(numberSeeds * ratioNumberSeeds).toInt
    val usedSeeds: List[Node] = seeds.take(cutoff)

    // create dataframes for traversal (up and down)
    val (up: DataFrame, down: DataFrame) = createDataframesToTraverse(df)

    // seeds in dataframe asstarting paths
    var paths: DataFrame = usedSeeds.map(_.toString).toDF("n_0").cache() // seedsDf.map(_.toString).limit(cutoff).toDF("n0")
    println(s"we start initially with following seeds:\n${usedSeeds.mkString("\n")}")

    // traverse up
    // println("traverse up")
    paths = traverse(paths, up, iterationLimit = maxUp, traverseDirection = "up").cache()

    // traverse down
    // println("traverse down")
    paths = traverse(paths, down, iterationLimit = maxDown, traverseDirection = "down").cache()

    // all gathered paths
    // println("gathered paths")
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

  def main(args: Array[String]): Unit = {

    val configFilePath = args(0)
    val config = new ConfigResolver(configFilePath).getConfig()

    println(config)

    val personFilePath: String = config.getString("personFilePath") // "/Users/carstendraschner/Downloads/test.ttl"
    val rdfFormat: String = config.getString("rdfFormat") // "turtle"

    val seedVarName = config.getString("seedVarName") // "?seed"
    val whereClauseForSeed = config.getString("whereClauseForSeed") // "?seed a <http://dig.isi.edu/Person>"

    val maxUp: Int = config.getInt("maxUp") // 5
    val maxDown: Int = config.getInt("maxDown") // 5

    val seedNumber: Int = config.getInt("seedNumber") // 0
    val seedNumberAsRatio: Double = config.getDouble("seedNumberAsRatio") // 1.0

    // setup spark session
    val spark = SparkSession.builder
      .appName(s"tryout sparql query transformer")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node)])

    // first mini file:
    val df = spark.read.rdf(Lang.TURTLE)(personFilePath) // TODO dynamic change of language


    val (totalSparqlQuery: String, var_names: List[String]) = autoPrepo(df, seedVarName, seedWhereClause = whereClauseForSeed, maxUp = maxUp, maxDown = maxDown)

    println(
      f"""
         |The automatic created feature extracting sparql fetched ${var_names.size} projection variables representing literals.
         |the projection variables are:
         |${var_names.map(vn => f"\t$vn").mkString("\n")}
         |\n
         |""".stripMargin)
    println(f"The resulting sparql query is: \n$totalSparqlQuery")
  }
}