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

  def create_seed_fetching_sparql(seed_var_name: String, seed_where_claus: String, sorted_by_links: Boolean): String = {
    val seed_fetching_sparql = sorted_by_links match {
      case true => f"SELECT DISTINCT $seed_var_name \nWHERE { $seed_where_claus \n\tOptional { $seed_var_name ?p ?o. } } \ngroup by $seed_var_name ORDER BY DESC ( count(?p) ) "
      case false => f"SELECT DISTINCT $seed_var_name \n WHERE { $seed_where_claus} }"
    }
    println(f"the generated seed fetching sparql is:\n$seed_fetching_sparql")
    seed_fetching_sparql
  }

  def create_dataframes_to_traverse(df: DataFrame): (DataFrame, DataFrame) = {
    df.toDF(Seq("s", "p", "o"): _*)
    df.printSchema()

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

  def traverse(paths: DataFrame, traverse_df: DataFrame, iteration_limit: Int, traverse_direction: String): DataFrame = {

    val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    var dataFramesWithOpenEnd: DataFrame = paths
    var dataframeWithLiteralEnd: DataFrame = spark.emptyDataFrame
    var currentPaths = paths

    breakable {
      for (iteration <- 0  to (iteration_limit-1)) {
        // set iterators
        val column_name = f"n_$iteration"
        val iterationPlusOne: Int = iteration + 1
        val column_name_plus_one = f"n_$iterationPlusOne"

        // paths to merge
        val left: DataFrame = currentPaths
        val right: DataFrame = traverse_df.toDF(Seq(f"n_$iteration", f"p_$iteration", f"n_$iterationPlusOne", f"dir_$iteration"): _*)

        // this joines the next hop
        // println(s"joinedPaths dataframe: $iteration")
        val joinedPaths = left.join(right, column_name)
        // joinedPaths.show(false)
        // current paths are the ones we want to follow in next iteration. so it is reasonable if   TODO better literal identification
        // they end with not literal.
        // see ! excamation mark in where statement
        // println(s"current paths dataframe: $iteration")
        currentPaths = joinedPaths.where(!col(column_name_plus_one).startsWith("\""))
        // final paths are paths which end with literal
        // this can only happen when traversing down
        val finalPaths = joinedPaths.where(col(column_name_plus_one).startsWith("\""))

        // filter out cyclic paths from currentPaths
        val noCycle = udf((row: Row) => {
          val l = row.toSeq.toList
            .filter(_!=None)
            .filter(_!=null)
          val lFromSet = l.toSet
          l.length == lFromSet.size
        })
        // ln(f"$iteration cyclic paths:")
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

        /* println(iteration)
        println()
        currentPaths.show(false)
        println()
        finalPaths.show(false)

         */
        if (currentPaths.count() == 0) {
          println(f"no remaining paths are available so: $traverse_direction is done")
          break
        }

        // if we traverse up we change column names s.t. last elment added is alway in column n0 s.t. join in traverse down is easier
        if (traverse_direction == "up") {
          val tmp_paths: DataFrame = currentPaths
          val tmp_columns = tmp_paths.columns.toSeq
          val new_tmp_columns: Seq[String] = tmp_columns.map(c => {
            val currentNumber: Int = c.split("_").last.toInt
            val currentChars: String = c.split("_")(0)
            val newNumber = (currentNumber - iterationPlusOne).toString
            val newColumnName = currentChars + "_" + newNumber
            // println(c, newColumnName)
            newColumnName
          })
          val recentColumns: Seq[String] = dataFramesWithOpenEnd.columns.toSeq
          val noneColumnsToAdd = new_tmp_columns.toSet.diff(recentColumns.toSet).toSeq
          var df1 = dataFramesWithOpenEnd
          for (c <- noneColumnsToAdd) df1 = df1.withColumn(c, lit(null: String))
          val  df2 = tmp_paths.toDF(new_tmp_columns: _*)
          val df3 = df2.union(df1.select(df2.columns.map(col(_)): _*))
          dataFramesWithOpenEnd = df3
        }
      }
    }
    val returnDataframe = traverse_direction match {
      case "up" => dataFramesWithOpenEnd
      case "down" => dataframeWithLiteralEnd
    }
    returnDataframe
  }

  def auto_prepo(df: DataFrame, seed_var_name: String, seed_where_claus: String, max_up: Int, max_down: Int, number_seeds: Int = 0, ratio_number_seeds: Double = 1.0): (String, List[String]) = {

    val spark = SparkSession.builder
      .getOrCreate()
    import spark.implicits._
    implicit val nodeEncoder = Encoders.kryo(classOf[Node])
    implicit val nodeTupleEncoder = Encoders.tuple[Node, Node, Node](nodeEncoder, nodeEncoder, nodeEncoder)

    val ds = df.toDS().cache()

    // create the sparql to reach seeds and maybe sort them by ths sparql as well
    val seed_fetching_sparql: String = create_seed_fetching_sparql(seed_var_name, seed_where_claus, sorted_by_links = true)

    // query for seeds and list those
    val querytransformer1: SPARQLQuery = SPARQLQuery(seed_fetching_sparql)
    val seedsDf: DataFrame = querytransformer1.transform(ds).cache()
    val seeds: List[Node] = seedsDf.as[Node].rdd.collect().toList
    println(f"the fetched seeds are: $seeds")
    val numberSeeds: Int = seeds.length

    // calculate cutoff
    var cutoff: Int = numberSeeds
    cutoff = if (number_seeds >= 0) number_seeds else cutoff
    cutoff = math.rint(numberSeeds * ratio_number_seeds).toInt
    val usedSeeds: List[Node] = seeds.take(cutoff)

    // create dataframes for traversal (up and down)
    val (up: DataFrame, down: DataFrame) = create_dataframes_to_traverse(df)

    // seeds in dataframe asstarting paths
    var paths: DataFrame = usedSeeds.map(_.toString).toDF("n_0").cache() // seedsDf.map(_.toString).limit(cutoff).toDF("n0")
    println("we start initially with following seeds:")
    paths.show(false)

    // traverse up
    // println("traverse up")
    paths = traverse(paths, up, iteration_limit = max_up, traverse_direction = "up").cache()
    // paths.show(false)

    // traverse down
    println("traverse down")
    paths = traverse(paths, down, iteration_limit = max_down, traverse_direction = "down").cache()

    // all gathered paths
    println("gathered paths")
    val columns = paths.columns.toList
    println(columns)
    val newColumnsOrder: Seq[String] = columns
      .map(_.split("_").last.toInt)
      .distinct
      .sorted
      .dropRight(1)
      .flatMap(i => (f"n_$i p_$i dir_$i n_${i + 1}").split(" "))
      .distinct
    // println(newColumnsOrder)
    paths = paths.select(newColumnsOrder.map(col(_)): _*).cache()
    paths.show(false)

    val results = paths.rdd.map(row => {
      // println(row)
      // val rowLength = row.size
      // (rowLength)
      val nonNullRow: List[String] = row.toSeq.toList.filter(_!=None).filter(_!=null).asInstanceOf[List[String]]
      // println(nonNullRow)

      // val indices = newColumnsOrder.map(_.split("_").last.toInt).distinct.sorted
      val lenRow: Int = nonNullRow.size
      val numberQueryLines: Int = (lenRow-1)/3.toInt

      var var_names = ListBuffer(seed_var_name)

      var projection_var: String = ""

      var queryStr = "\tOPTIONAL {\n"

      // println(nonNullRow)
      for (queryLineNumber <- 0 to (numberQueryLines-1)) {
        // println(queryLineNumber, numberQueryLines)
        val leftN = nonNullRow(queryLineNumber * 3)
        val p = nonNullRow((queryLineNumber * 3) + 1)
        val direction = nonNullRow((queryLineNumber * 3) + 2)
        val rightN = nonNullRow((queryLineNumber * 3) + 3)

        var first_var_name = var_names.last
        var second_var_name = first_var_name + f"__$direction" + "_" + p.toString.split("/").last.replace("#", "_")
        var_names.append(second_var_name)
        val query_line: String = direction match {
          case "down" => f"$first_var_name <$p> $second_var_name ."
          case "up" => f"$second_var_name <$p> $first_var_name ."
        }
        queryStr = queryStr + f"\t\t$query_line\n"
        projection_var = second_var_name
        // println(query_line)
      }
      queryStr = queryStr + "\t}"

      (queryStr, projection_var)
    }).cache()

    val queryLines: List[String] = results.map(_._1.toString).collect().toList.distinct.sortBy(_.size)
    val projectionVars: List[String] = results.map(_._2.toString).collect().toList.distinct.sortBy(_.size)

    val projection_vars_string = projectionVars.mkString(" ")
    print(f"number projection vars: ${projectionVars.size}")
    val all_optional_query_blocks_str = queryLines.mkString("\n")
    val total_query = f"SELECT $seed_var_name $projection_vars_string\n\nWHERE {\n\t${seed_where_claus}\n\n${all_optional_query_blocks_str} \n}}"

    (total_query, projectionVars)
  }

  def main(args: Array[String]): Unit = {

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
    val person_file_path = "/Users/carstendraschner/Downloads/test.ttl"
    val df = spark.read.rdf(Lang.TURTLE)(person_file_path)

    df.printSchema()

    df.show(false)

    val seed_var_name = "?seed"
    val where_clause_for_seed = "?seed a <http://dig.isi.edu/Person>"

    val max_up: Int = 5
    val max_down: Int = 5

    val seedNumber: Int = 0
    val seedNumberAsRatio: Double = 1.0

    val (total_sparql_query: String, var_names: List[String]) = auto_prepo(df, seed_var_name, seed_where_claus = where_clause_for_seed, max_up = max_up, max_down = max_down)

    println(f"The automatic created feature extracting sparql fetched: ${var_names.size} projection variables representing literals")
    println(f"the projection variables are:")
    var_names.map(vn => f"\t$vn").foreach(println(_))
    println()
    println(f"The resulting sparql query is: ")
    println(total_sparql_query)
  }
}