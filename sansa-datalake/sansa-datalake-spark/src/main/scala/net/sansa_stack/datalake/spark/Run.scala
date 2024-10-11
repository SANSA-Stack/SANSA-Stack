package net.sansa_stack.datalake.spark

import java.io.FileNotFoundException

import com.typesafe.scalalogging.Logger
import net.sansa_stack.datalake.spark.utils.Helpers
import net.sansa_stack.datalake.spark.utils.Helpers._
import org.apache.spark.sql.DataFrame

import scala.jdk.CollectionConverters._
import scala.collection.mutable


class Run[A](executor: QueryExecutor[A]) {

  private var finalDataSet: A = _

  def application(queryFile: String, mappingsFile: String, configFile: String): DataFrame = {

    val logger = Logger("SANSA-DataLake")

    // 1. Read SPARQL query
    logger.info("QUERY ANALYSIS starting...")

    try {
      var query = Helpers.readFileFromPath(queryFile)

      logger.info(s"Going to execute the query:\n$query")

      // Transformations
      var transformExist = false
      var transformationsInLine = ""
      if (query.contains("TRANSFORM")) {
        transformationsInLine = query.substring(query.indexOf("TRANSFORM") + 9, query.lastIndexOf(")")) // E.g. ?k?a.toInt && ?a?l.r.toInt.scl(_+61)
        query = query.replace("TRANSFORM" + transformationsInLine + ")", "") // TRANSFORM is not defined in Jena, so remove
        transformExist = true
      }

      // 2. Extract star-shaped BGPs
      val qa = new QueryAnalyser(query)


      val stars = qa.getStars
      val starsNbr = stars._1.size

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

      logger.info(s"Predicate Star: $variablePredicateStar")

      val prefixes = qa.getPrefixes
      val (select, distinct) = qa.getProject
      val filters = qa.getFilters
      val orderBys = qa.getOrderBy
      val groupBys = qa.getGroupBy(variablePredicateStar, prefixes)

      var limit: Int = 0
      if (qa.hasLimit) limit = qa.getLimit

      logger.info("- Predicates per star:")

      val star_predicate_var = stars._2 // TODO: assuming no (star,predicate) with two vars?
      logger.info("star_predicate_var: " + star_predicate_var)

      // 3. Generate plan of joins
      logger.info("PLAN GENERATION & MAPPINGS")
      val pl = new Planner(stars._1)
      val pln = pl.generateJoinPlan
      val joins = pln._1
      val joinedToFlag = pln._2
      val joinedFromFlag = pln._3
      val joinPairs = pln._4

      // 4. Check mapping file
      logger.info("---> MAPPING CONSULTATION")

      val mappers = new Mapper(mappingsFile)
      val results = mappers.findDataSources(stars._1, configFile)

      val neededPredicates = pl.getNeededPredicates(star_predicate_var, joins, select, groupBys, prefixes)
      val neededPredicatesAll = neededPredicates._1 // all predicates used
      val neededPredicatesSelect = neededPredicates._2 // only projected out predicates

      logger.info("--> Needed predicates all: " + neededPredicatesAll)

      var star_df: Map[String, A] = Map.empty
      var starNbrFilters: Map[String, Integer] = Map()

      var starDataTypesMap: Map[String, mutable.Set[String]] = Map()
      val parsetIDs: Map[String, String] = Map() // Used when subject variables are projected out

      logger.info("---> GOING NOW TO COLLECT DATA")

      for (s <- results) {
        val star = s._1
        logger.info("star: " + star)
        val dataSources = s._2
        val options = s._3

        val dataTypes = dataSources.map(d => d._3)

        // 'Mappings' transformations
        for (ds <- dataSources) {
            val transformations = ds._4

            if (transformations.nonEmpty) {
                transformExist = true
            }

          for (t <- transformations) {
                logger.info("Visiting transformation related to predicate: " + t._1 + " =  " + t._2)
                val fncParamBits = t._2._1.split(" ")
                val fncName = fncParamBits(0)
                var fncParam = ""

                if (fncParamBits.size > 2) { // E.g., skip 2 producerID
                    fncParam = fncParamBits(1)
                } // otherwise, it's 1 parameter, e.g., toInt producerID

                val IDorNot = t._2._2
                var lOrR = ""
                lOrR = if (IDorNot) "l" else "r"

                // Construct the in-line transformation declarations (like 'SPARQL' transformations)
                joinPairs.keys.foreach(
                    x => if (omitQuestionMark(star) == x._1 && joinPairs(x) == t._1) { // Case of predicate transformations
                        if (transformationsInLine != "") {
                          transformationsInLine += " && "
                        }
                      transformationsInLine += s"?${x._1}?${x._2}.$lOrR.${getFunctionFromURI(fncName)}"
                        if (fncParam != "") {
                            transformationsInLine += s"($fncParam)"
                        }
                    } else if (omitQuestionMark(star) == x._2) { // Case of ID transformations
                        if (transformationsInLine != "") {
                          transformationsInLine += " && "
                        }
                        transformationsInLine += s"?${x._1}?${x._2}.$lOrR.${getFunctionFromURI(fncName)}"
                        if (fncParam != "") {
                            transformationsInLine += s"($fncParam)"
                        }
                    }
                )
            }
        }

        if (transformationsInLine != "") {
            logger.info(s"Transformations found (inline): $transformationsInLine")
        }

        starDataTypesMap += (star -> dataTypes)
        var parsetIDs : Map[String, String] = Map()

        logger.info("Getting DF relevant to the star: " + star)

        // Transformations
        var leftJoinTransformations: (String, Array[String]) = null
        var rightJoinTransformations: Array[String] = null
        if (transformExist) {
          val (transmap_left, transmap_right) = qa.getTransformations(transformationsInLine)

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
          val (ds, numberOfFiltersOfThisStar, parsetID) = executor.query(dataSources, options, toJoinWith = true, star, prefixes,
            select, star_predicate_var, neededPredicatesAll, filters, leftJoinTransformations, rightJoinTransformations,
            joinPairs)

          if (parsetID != "") {
            parsetIDs += (star -> parsetID)
          }

          star_df += (star -> ds) // DataFrame representing a star

          starNbrFilters += star -> numberOfFiltersOfThisStar

          logger.info("join...with ParSet schema: " + ds)
        } else if (!joinedToFlag.contains(star) && !joinedFromFlag.contains(star)) {
          val (ds, numberOfFiltersOfThisStar, parsetID) = executor.query(dataSources, options, toJoinWith = false, star, prefixes,
            select, star_predicate_var, neededPredicatesAll, filters, leftJoinTransformations, rightJoinTransformations,
            joinPairs)

          // ds.printSchema() // SEE WHAT TO DO HERE TO SHOW BACK THE SCHEMA - MOVE IN SPARKEXECUTOR

          parsetIDs += (star -> parsetID)
          star_df += (star -> ds) // DataFrame representing a star

          starNbrFilters += star -> numberOfFiltersOfThisStar

          logger.info("single...with ParSet schema: " + ds)
        }
      }

      logger.info("QUERY EXECUTION starting...")
      logger.info(s"DataFrames: $star_df")

      if (starsNbr > 1) {
        logger.info(s"- Here are the (Star, ParSet) pairs:")
        logger.info("Join Pairs: " + joinPairs)

        if (starsNbr > 1) logger.info(s"- Here are join pairs: $joins") else logger.info("No join detected.")
        logger.info(s"- Number of predicates per star: $starNbrFilters ")

        val starWeights = pl.sortStarsByWeight(starDataTypesMap, starNbrFilters, configFile)
        logger.info(s"- Stars weighted (performance + nbr of filters): $starWeights")

        val sortedScoredJoins = pl.reorder(joins, starDataTypesMap, starNbrFilters, starWeights, configFile)
        logger.info(s"- Sorted scored joins: $sortedScoredJoins")
        val startingJoin = sortedScoredJoins.head

        // Convert starting join to: (leftStar, (rightStar, joinVar)) so we can remove it from $joins
        var firstJoin: (String, (String, String)) = null
        for (j <- joins.entries.asScala) {
          if (j.getKey == startingJoin._1._1 && j.getValue._1 == startingJoin._1._2) {
            firstJoin = startingJoin._1._1 -> (startingJoin._1._2, j.getValue._2)
          }
        }
        logger.info(s"- Starting join: $firstJoin")

        finalDataSet = executor.join(joins, prefixes, star_df)

        // finalDataSet.asInstanceOf[DataFrame].printSchema()

                    // finalDataSet = executor.joinReordered(joins, prefixes, star_df, firstJoin, starWeights)
      } else {
          logger.info(s" Single star query")
          finalDataSet = star_df.head._2
      }

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

      // Add subjects
      for (i <- parsetIDs) {
          val star = i._1
          val parsetID = i._2

          columnNames = columnNames :+  s"${omitQuestionMark(star)}"
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

      // TODO: check the order of PROJECT and ORDER-BY
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

        // TODO: (-1 ASC, -2 DESC) confirm with multiple order-by's
        logger.info(s"ORDER BY list: $orderByList (-1 ASC, -2 DESC)")

        for (o <- orderByList) {
          val variable = o._1
          val direction = o._2

          finalDataSet = executor.orderBy(finalDataSet, direction, variable)
        }
      }

      logger.info("|__ Has distinct? " + distinct)
      finalDataSet = executor.project(finalDataSet, columnNames, distinct)

      if (limit > 0) {
        finalDataSet = executor.limit(finalDataSet, limit)
      }

//      executor.run(finalDataSet)

      finalDataSet.asInstanceOf[DataFrame]

    } catch {
      case ex : FileNotFoundException =>
        println("ERROR: One of input files ins't found (Report it: " + ex + ")")
        logger.debug(ex.getStackTrace.toString)
        null

      case ex : org.apache.jena.riot.RiotException =>
        println("ERROR: invalid Mappings. Check syntax. (Report it: " + ex + ")")
        logger.debug(ex.getStackTrace.toString)
        null

      case ex : org.apache.spark.SparkException =>
        println("ERROR: invalid Spark Master. (Report it: " + ex + ")")
        logger.debug(ex.getStackTrace.toString)
        null

      case ex : com.fasterxml.jackson.core.JsonParseException =>
        println("ERROR: invalid JSON content in config file. (Report it: " + ex + ")")
        logger.debug(ex.getStackTrace.toString)
        null

      case ex : java.lang.IllegalArgumentException =>
        println("ERROR: invalid mappings. (Report it: " + ex + ")")
        logger.debug(ex.getStackTrace.toString)
        null

      case ex : org.apache.jena.query.QueryParseException =>
        println("ERROR: invalid query. (Report it: " + ex + ")")
        logger.debug(ex.getStackTrace.toString)
        null

      case ex : com.amazonaws.services.s3.model.AmazonS3Exception =>
        println(ex.getStackTrace)
        println("ERROR: Access to Amazon S3 denied. Check bucket name and key. Check you have ~/.aws/credentials file " +
          "with the correct content: \n[default]\naws_access_key_id=...\naws_secret_access_key=...")
        null
    }
  }
}
