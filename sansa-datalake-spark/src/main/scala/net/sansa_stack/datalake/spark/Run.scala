package net.sansa_stack.datalake.spark

import com.typesafe.scalalogging.Logger
import java.io.FileNotFoundException

import org.apache.commons.lang.time.StopWatch
import org.apache.spark.sql.DataFrame
import net.sansa_stack.datalake.spark.utils.Helpers._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by mmami on 26.01.17.
 */
class Run[A](executor: QueryExecutor[A]) {

  private var finalDataSet: A = _

  def application(queryFile: String, mappingsFile: String, configFile: String): DataFrame = {

    val logger = Logger("SANSA-DataLake")

    // 1. Read SPARQL query
    logger.info("QUERY ANALYSIS startigng...")

    try {
      var query = ""
      if (queryFile.startsWith("hdfs://")) {
        val host_port = queryFile.split("/")(2).split(":")
        val host = host_port(0)
        val port = host_port(1)
        val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://" + host + ":" + port + "/"), new org.apache.hadoop.conf.Configuration())
        val path = new org.apache.hadoop.fs.Path(queryFile)
        val stream = hdfs.open(path)
        def readLines = scala.io.Source.fromInputStream(stream)

        query = readLines.mkString
      } else if (queryFile.startsWith("s3")) { // E.g., s3://sansa-datalake/Q1.sparql
        val bucket_key = queryFile.replace("s3://","").split("/")

        val bucket = bucket_key.apply(0) // apply(x) = (x)
        val key = if (bucket_key.length > 2) bucket_key.slice(1, bucket_key.length).mkString("/") else bucket_key(1) // Case of folder

        import com.amazonaws.services.s3.AmazonS3Client
        import com.amazonaws.services.s3.model.GetObjectRequest
        import java.io.BufferedReader
        import java.io.InputStreamReader
        import scala.collection.JavaConversions._

        val s3 = new AmazonS3Client
        val s3object = s3.getObject(new GetObjectRequest(bucket, key))

        val reader: BufferedReader = new BufferedReader(new InputStreamReader(s3object.getObjectContent))
        val lines = new ArrayBuffer[String]()
        var line: String = null
        while ({line = reader.readLine; line != null}) {
          lines.add(line)
        }
        reader.close()
        lines.toArray

        query = lines.mkString("\n")
      } else {
          var queryFromFile = scala.io.Source.fromFile(queryFile)
          query = try queryFromFile.mkString finally queryFromFile.close()
      }

      println(s"Going to execute the query:\n$query")

      // Transformations
      var transformExist = false
      var trans = ""
      if (query.contains("TRANSFORM")) {
        trans = query.substring(query.indexOf("TRANSFORM") + 9, query.lastIndexOf(")")) // E.g. ?k?a.toInt && ?a?l.r.toInt.scl(_+61)
        query = query.replace("TRANSFORM" + trans + ")", "") // TRANSFORM is not defined in Jena, so remove
        transformExist = true
      }

      // 2. Extract star-shaped BGPs
      val qa = new QueryAnalyser(query)

      val stars = qa.getStars

      // Create a map between the variable and its star and predicate URL [variable -> (star,predicate)]
      // Need e.g. to create the column to 'SQL ORDER BY' from 'SPARQL ORDER BY'
      var variablePredicateStar: Map[String, (String, String)] = Map()
      for (v <- stars._1) {
        val star = v._1
        val predicate_variable_set = v._2
        for (pv <- predicate_variable_set) {
          val predicate = pv._1
          val variable = pv._2

          variablePredicateStar += (variable -> (star, predicate))
        }
      }

      logger.info(s"predicateStar: $variablePredicateStar")

      val prefixes = qa.getPrefixes
      val (select, distinct) = qa.getProject
      val filters = qa.getFilters
      val orderBys = qa.getOrderBy
      val groupBys = qa.getGroupBy(variablePredicateStar, prefixes)

      var limit: Int = 0
      if (qa.hasLimit) limit = qa.getLimit()

      logger.info("- Predicates per star:")

      val star_predicate_var = stars._2 // TODO: assuming no (star,predicate) with two vars?
      logger.info("star_predicate_var: " + star_predicate_var)

      // 3. Generate plan of joins
      logger.info("PLAN GENERATION & MAPPINGS starting...")
      val pl = new Planner(stars._1)
      val pln = pl.generateJoinPlan
      val joins = pln._1
      val joinedToFlag = pln._2
      val joinedFromFlag = pln._3
      val joinPairs = pln._4

      val neededPredicates = pl.getNeededPredicates(star_predicate_var, joins, select)
      val neededPredicatesAll = neededPredicates._1 // all predicates used
      val neededPredicatesSelect = neededPredicates._2 // only projected out predicates

      logger.info("--> Needed predicates all: " + neededPredicatesAll)

      // 4. Check mapping file
      logger.info("---> MAPPING CONSULTATION")

      val mappers = new Mapper(mappingsFile)
      val results = mappers.findDataSources(stars._1, configFile)

      var star_df: Map[String, A] = Map.empty
      var starNbrFilters: Map[String, Integer] = Map()

      var starDataTypesMap: Map[String, mutable.Set[String]] = Map()

      logger.info("---> GOING NOW TO JOIN STUFF")
      for (s <- results) {
        val star = s._1
        val datasources = s._2
        val options = s._3

        val dataTypes = datasources.map(d => d._3)

        starDataTypesMap += (star -> dataTypes)

        logger.info("* Getting DF relevant to the star: " + star)

        // Transformations
        var leftJoinTransformations: (String, Array[String]) = null
        var rightJoinTransformations: Array[String] = null
        if (transformExist) {
          val (transmap_left, transmap_right) = qa.getTransformations(trans)

          val str = omitQuestionMark(star)
          if (transmap_left.keySet.contains(str)) {
            // Get with whom there is a join
            val rightOperand = transmap_left(str)._1
            val ops = transmap_left(str)._2

            // Get the predicate of the join
            val joinLeftPredicate = joinPairs((str, rightOperand))
            leftJoinTransformations = (joinLeftPredicate, ops)
            logger.info("Transform (left) on predicate " + joinLeftPredicate + " using " + ops.mkString("_"))
          }

          if (transmap_right.keySet.contains(str)) {
            rightJoinTransformations = transmap_right(str)
            logger.info("Transform (right) ID using " + rightJoinTransformations.mkString("..."))
          }
        }

        if (joinedToFlag.contains(star) || joinedFromFlag.contains(star)) {
          val (ds, numberOfFiltersOfThisStar) = executor.query(datasources, options, true, star, prefixes, select, star_predicate_var, neededPredicatesAll, filters, leftJoinTransformations, rightJoinTransformations, joinPairs)

          star_df += (star -> ds) // DataFrame representing a star

          starNbrFilters += star -> numberOfFiltersOfThisStar

          logger.info("...with DataFrame schema: " + ds)
        } else if (!joinedToFlag.contains(star) && !joinedFromFlag.contains(star)) {
          val (ds, numberOfFiltersOfThisStar) = executor.query(datasources, options, false, star, prefixes, select, star_predicate_var, neededPredicatesAll, filters, leftJoinTransformations, rightJoinTransformations, joinPairs)

          star_df += (star -> ds) // DataFrame representing a star

          starNbrFilters += star -> numberOfFiltersOfThisStar

          logger.info("...with DataFrame schema: " + ds)
        }
      }

      logger.info("QUERY EXECUTION...")
      logger.info("- Here are the (Star, ParSet) pairs:")
      logger.info(s"  $star_df")
      logger.info(s"- Here are join pairs: $joins")
      logger.info(s"- Number of predicates per star: $starNbrFilters ")

      val starWeights = pl.sortStarsByWeight(starDataTypesMap, starNbrFilters, configFile)
      logger.info(s"- Stars weighted (performance + nbr of filters): $starWeights")

      val sortedScoredJoins = pl.reorder(joins, starDataTypesMap, starNbrFilters, starWeights, configFile)
      logger.info(s"- Sorted scored joins: $sortedScoredJoins")
      val startingJoin = sortedScoredJoins.head

      // Convert starting join to: (leftStar, (rightStar, joinVar)) so we can remove it from $joins
      var firstJoin: (String, (String, String)) = null
      for (j <- joins.entries) {
        if (j.getKey == startingJoin._1._1 && j.getValue._1 == startingJoin._1._2) {
          firstJoin = startingJoin._1._1 -> (startingJoin._1._2, j.getValue._2)
        }
      }
      logger.info(s"- Starting join: $firstJoin")

      finalDataSet = executor.join(joins, prefixes, star_df)

      // Project out columns from the final global join results
      var columnNames = Seq[String]()
      logger.info(s"--> Needed predicates select: $neededPredicatesSelect")
      for (i <- neededPredicatesSelect) {
        val star = i._1
        val ns_predicate = i._2
        val bits = get_NS_predicate(ns_predicate)

        val selected_predicate = omitQuestionMark(star) + "_" + bits._2 + "_" + prefixes(bits._1)
        columnNames = columnNames :+ selected_predicate
      }

      if (groupBys != null) {
        logger.info(s"groupBys: $groupBys")
        finalDataSet = executor.groupBy(finalDataSet, groupBys)

        // Add aggregation columns to the final project ones
        for (gb <- groupBys._2) {
          logger.info("-> Add to Project list:" + gb._2)
          columnNames = columnNames :+ gb._2 + "(" + gb._1 + ")"
        }
      }

      logger.info(s"SELECTED column names: $columnNames")

      if (orderBys != null) {
        logger.info(s"orderBys: $orderBys")

        var orderByList: Set[(String, String)] = Set()
        for (o <- orderBys) {
          val orderDirection = o._1
          val str = variablePredicateStar(o._2)._1
          val vr = variablePredicateStar(o._2)._2
          val ns_p = get_NS_predicate(vr)
          val column = omitQuestionMark(str) + "_" + ns_p._2 + "_" + prefixes(ns_p._1)
          orderByList += ((column, orderDirection))
        }

        logger.info(s"ORDER BY list: $orderByList (-1 ASC, -2 DESC)")

        for (o <- orderByList) {
          val variable = o._1
          val direction = o._2

          finalDataSet = executor.orderBy(finalDataSet, direction, variable)
        }
      }

      logger.info("|__ Has distinct? " + distinct)
      finalDataSet = executor.project(finalDataSet, columnNames, distinct)

      if (limit > 0) finalDataSet = executor.limit(finalDataSet, limit)

      logger.info("- Final results schema: ")

      val stopwatch: StopWatch = new StopWatch
      stopwatch.start

      executor.run(finalDataSet)

      stopwatch.stop

      val timeTaken = stopwatch.getTime

      println(s"Time of query execution: $timeTaken ms")

      finalDataSet.asInstanceOf[DataFrame]

    } catch {
      case ex : FileNotFoundException =>
        println("ERROR: One of input files ins't found." + ex.getStackTraceString)
        logger.debug(ex.getStackTraceString)
        null

      case ex : org.apache.jena.riot.RiotException =>
        println("ERROR: invalid Mappings. Check syntax. (Report it: " + ex + ").")
        logger.debug(ex.getStackTraceString)
        null

      case ex : org.apache.spark.SparkException =>
        println("ERROR: invalid Spark Master. (Report it: " + ex + ")")
        logger.debug(ex.getStackTraceString)
        null

      case ex : com.fasterxml.jackson.core.JsonParseException =>
        println("ERROR: invalid JSON content in config file. (Report it: " + ex + ")")
        logger.debug(ex.getStackTraceString)
        null

      case ex : java.lang.IllegalArgumentException =>
        println("ERROR: invalid mappings. (Report it: " + ex + ")")
        logger.debug(ex.getStackTraceString)
        null

      case ex : org.apache.jena.query.QueryParseException =>
        println("ERROR: invalid query. (Report it: " + ex + ")")
        logger.debug(ex.getStackTraceString)
        null

      case ex : com.amazonaws.services.s3.model.AmazonS3Exception =>
        println(ex.getStackTraceString)
        println("ERROR: Access to Amazon S3 denied. Check bucket name and key. Check you have ~/.aws/credentials file " +
          "with the correct content: \n[default]\naws_access_key_id=...\naws_secret_access_key=...")
        null
    }
  }
}
