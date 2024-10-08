package net.sansa_stack.datalake.spark

import java.util
import com.google.common.collect.ArrayListMultimap
import com.mongodb.spark.sql.connector.config.{MongoConfig, ReadConfig}
import com.typesafe.scalalogging.Logger
import net.sansa_stack.datalake.spark.utils.Helpers._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}

import scala.jdk.CollectionConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


class SparkExecutor(spark: SparkSession, mappingsFile: String) extends QueryExecutor[DataFrame] {

    override val logger = Logger("SANSA-DataLake")

    def getType: DataFrame = {
        val dataframe : DataFrame = null
        dataframe
    }

    def query(sources : mutable.Set[(mutable.HashMap[String, String], String, String, mutable.HashMap[String, (String, Boolean)])],
              optionsMap_entity: mutable.HashMap[String, (Map[String, String], String)],
               toJoinWith: Boolean,
               star: String,
               prefixes: Map[String, String],
               select: util.List[String],
               star_predicate_var: mutable.HashMap[(String, String), String],
               neededPredicates: mutable.Set[String],
               filters: ArrayListMultimap[String, (String, String)],
               leftJoinTransformations: (String, Array[String]),
               rightJoinTransformations: Array[String],
               joinPairs: Map[(String, String), String]
        ): (DataFrame, Integer, String) = {

        spark.sparkContext.setLogLevel("ERROR")

        var finalDF : DataFrame = null
        var dataSource_count = 0
        var parSetId = "" // To use when subject (thus ID) is projected out in SELECT

        for (s <- sources) {
            logger.info("NEXT SOURCE...")
            dataSource_count += 1 // in case of multiple relevant data sources to union

            val attr_predicate = s._1
            logger.info("Star: " + star)
            logger.info("Attribute_predicate: " + attr_predicate)
            val sourcePath = s._2
            val sourceType = getTypeFromURI(s._3)
            logger.info("sourcePathsourcePath: " + sourcePath)
            val options = optionsMap_entity(sourcePath)._1 // entity is not needed here in SparkExecutor

            // TODO: move to another class better
            var columns = getSelectColumnsFromSet(attr_predicate, omitQuestionMark(star), prefixes, select, star_predicate_var, neededPredicates, filters)

            val str = omitQuestionMark(star)

            if (select.contains(str)) {
                parSetId = getID(sourcePath, mappingsFile)
                columns = s"$parSetId AS `$str`, " + columns
            }

            logger.info("Relevant source (" + dataSource_count + ") is: [" + sourcePath + "] of type: [" + sourceType + "]")

            logger.info(s"...from which columns ($columns) are going to be projected")
            logger.info(s"...with the following configuration options: $options" )

            if (toJoinWith) { // That kind of table that is the 1st or 2nd operand of a join operation
                val id = getID(sourcePath, mappingsFile)
                logger.info(s"...is to be joined with using the ID: ${str}_id (obtained from subjectMap)")
                if (columns == "") {
                    columns = id + " AS " + str + "_ID"
                } else {
                    columns = columns + "," + id + " AS " + str + "_ID"
                }
            }

            logger.info("sourceType: " + sourceType)

            var df : DataFrame = null
            sourceType match {
                case "csv" => df = spark.read.options(options).csv(sourcePath)
                case "parquet" => df = spark.read.options(options).parquet(sourcePath)
                case "cassandra" =>
                    df = spark.read.format("org.apache.spark.sql.cassandra").options(options).load
                // case "elasticsearch" => // Will be enabled again when a Scala 2.12 version is provided.
                //    df = spark.read.format("org.elasticsearch.spark.sql").options(options).load
                case "mongodb" =>
                    // spark.conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
                    val values = options.values.toList
                    val mongoConf = if (values.length == 4) makeMongoURI(values(0), values(1), values(2), values(3))
                                    else makeMongoURI(values(0), values(1), values(2), null)
                    val mongoOptions: ReadConfig = MongoConfig.readConfig(Map("uri" -> mongoConf, "partitioner" -> "MongoPaginateBySizePartitioner").asJava)
                    df = spark.read.format("com.mongodb.spark.sql").options(mongoOptions.getOptions).load
                case "jdbc" =>
                    df = spark.read.format("jdbc").options(options).load()
                case "rdf" =>
                    val rdf = new NTtoDF()
                    df = rdf.options(options.asJava).read(sourcePath, spark).toDF()
                case _ =>
            }

            df.createOrReplaceTempView("table")
            try {
                val newDF = spark.sql("SELECT " + columns + " FROM table")

                if (dataSource_count == 1) {
                    finalDF = newDF
                } else {
                    finalDF = finalDF.union(newDF)
                }
            } catch {
                case ae: AnalysisException => val logger = println("ERROR: There is a mismatch between the mappings, query and/or data. " +
                  "Examples: Check `rr:reference` references a correct attribute, or if you have transformations, " +
                  "Check `rml:logicalSource` is the same between the TripleMap and the FunctionMap. Check if you are " +
                  "SELECTing a variable used in the graph patterns. Returned error is:\n" + ae)
                   System.exit(1)
            }

            // Transformations
            if (leftJoinTransformations != null && leftJoinTransformations._2 != null) {
                val column: String = leftJoinTransformations._1
                logger.info("Left Join Transformations: " + column + " - " + leftJoinTransformations._2.mkString("."))
                val ns_pred = get_NS_predicate(column)
                val ns = prefixes(ns_pred._1)
                val pred = ns_pred._2
                val col = str + "_" + pred + "_" + ns
                finalDF = transform(finalDF, col, leftJoinTransformations._2)

            }
            if (rightJoinTransformations != null && !rightJoinTransformations.isEmpty) {
                logger.info("right Join Transformations: " + rightJoinTransformations.mkString("_"))
                val col = str + "_ID"
                finalDF = transform(finalDF, col, rightJoinTransformations)
            }
        }

        logger.info("- filters: " + filters + " for star " + star)

        var whereString = ""

        var nbrOfFiltersOfThisStar = 0

        val it = filters.keySet().iterator()
        while (it.hasNext) {
            val value = it.next()
            val predicate = star_predicate_var.
                filter(t => t._2 == value).
                keys. // To obtain (star, predicate) pairs having as value the FILTER'ed value
                filter(t => t._1 == star).
                map(f => f._2).toList

            if (predicate.nonEmpty) {
                val ns_p = get_NS_predicate(predicate.head) // Head because only one value is expected to be attached to the same star an same (object) variable
                val column = omitQuestionMark(star) + "_" + ns_p._2 + "_" + prefixes(ns_p._1)
                logger.info("--- Filter column: " + column)

                nbrOfFiltersOfThisStar = filters.get(value).size()

                val conditions = filters.get(value).iterator()
                while (conditions.hasNext) {
                    val operand_value = conditions.next()
                    logger.info("--- Operand - Value: " + operand_value)
                    whereString = column + operand_value._1 + operand_value._2
                    logger.info("--- WHERE string: " + whereString)


                    if (operand_value._1 != "regex") {
                        try {
                            finalDF = finalDF.filter(whereString)
                        } catch {
                            case ae: NullPointerException => val logger = println("ERROR: No relevant source detected.")
                                System.exit(1)
                        }
                    } else {
                        finalDF = finalDF.filter(finalDF(column).like(operand_value._2.replace("\"", "")))
                        // regular expression with _ matching an arbitrary character and % matching an arbitrary sequence
                    }
                }
            }
        }

        logger.info(s"Number of filters of this star is: $nbrOfFiltersOfThisStar")

        (finalDF, nbrOfFiltersOfThisStar, parSetId)
    }

    def transform(df: Any, column: String, transformationsArray : Array[String]): DataFrame = {

        var ndf : DataFrame = df.asInstanceOf[DataFrame]
        for (t <- transformationsArray) {
            logger.info("Transformation next: " + t)
            t match {
                case "toInt" =>
                    logger.info("TOINT found")
                    ndf = ndf.withColumn(column, ndf(column).cast(IntegerType))
                    // From SO: values not castable will become null
                case s if s.contains("scl") =>
                    val scaleValue = s.replace("scl", "").trim.stripPrefix("(").stripSuffix(")")
                    logger.info("SCL found: " + scaleValue)
                    val operation = scaleValue.charAt(0)
                    operation match {
                        case '+' => ndf = ndf.withColumn(column, ndf(column) + scaleValue.substring(1).toInt)
                        case '-' => ndf = ndf.withColumn(column, ndf(column) - scaleValue.substring(1).toInt)
                        case '*' => ndf = ndf.withColumn(column, ndf(column) * scaleValue.substring(1).toInt)
                    }
                case s if s.contains("skp") =>
                    val skipValue = s.replace("skp", "").trim.stripPrefix("(").stripSuffix(")")
                    logger.info("SKP found: " + skipValue)
                    ndf = ndf.filter(!ndf(column).equalTo(skipValue))
                case s if s.contains("substit") =>
                    val replaceValues = s.replace("substit", "").trim.stripPrefix("(").stripSuffix(")").split("\\,")
                    val valToReplace = replaceValues(0)
                    val valToReplaceWith = replaceValues(1)
                    logger.info("SUBSTIT found: " + replaceValues.mkString(" -> "))
                    ndf = ndf.withColumn(column, when(col(column).equalTo(valToReplace), valToReplaceWith))

                case s if s.contains("replc") =>
                    val replaceValues = s.replace("replc", "").trim.stripPrefix("(").stripSuffix(")").split("\\,")
                    val valToReplace = replaceValues(0).replace("\"", "")
                    val valToReplaceWith = replaceValues(1).replace("\"", "")
                    logger.info("REPLC found: " + replaceValues.mkString(" -> ") + " on column: " + column)
                    ndf = ndf.withColumn(column, when(col(column).contains(valToReplace), regexp_replace(ndf(column), valToReplace, valToReplaceWith)))
                case s if s.contains("prefix") =>
                    val prefix = s.replace("prfix", "").trim.stripPrefix("(").stripSuffix(")")
                    logger.info("PREFIX found: " + prefix)
                    ndf = ndf.withColumn(column, concat(lit(prefix), ndf.col(column)))
                case s if s.contains("postfix") =>
                    val postfix = s.replace("postfix", "").trim.stripPrefix("(").stripSuffix(")")
                    logger.info("POSTFIX found: " + postfix)
                    ndf = ndf.withColumn(column, concat(lit(ndf.col(column), postfix)))
                case _ =>
            }
        }

        ndf
    }

    def join(joins: ArrayListMultimap[String, (String, String)], prefixes: Map[String, String], star_df: Map[String, DataFrame]): DataFrame = {
        import scala.collection.JavaConverters._
        import scala.collection.mutable.ListBuffer

        var pendingJoins = mutable.Queue[(String, (String, String))]()
        val seenDF : ListBuffer[(String, String)] = ListBuffer()
        var firstTime = true
        val join = " x "
        var jDF : DataFrame = null

        val it = joins.entries.iterator
        while ({it.hasNext}) {
            val entry = it.next

            val op1 = entry.getKey
            val op2 = entry.getValue._1
            val jVal = entry.getValue._2

            logger.info(s"-> GOING TO JOIN ($op1 $join $op2) USING $jVal...")

            val njVal = get_NS_predicate(jVal)
            val ns = prefixes(njVal._1)

            it.remove()

            val df1 = star_df(op1)
            val df2 = star_df(op2)

            if (firstTime) { // First time look for joins in the join hashmap
                logger.info("...that's the FIRST JOIN")
                seenDF.addOne((op1, jVal))
                seenDF.addOne((op2, "ID"))
                firstTime = false

                // Join level 1
                try {
                    jDF = df1.join(df2, df1.col(omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns).equalTo(df2(omitQuestionMark(op2) + "_ID")))
                    logger.info("...done")
                } catch {
                    case ae: NullPointerException => val logger = println("ERROR: No relevant source detected.")
                        System.exit(1)
                }

            } else {
                val dfs_only = seenDF.map(_._1)
                logger.info(s"EVALUATING NEXT JOIN ...checking prev. done joins: $dfs_only")
                if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
                    logger.info("...we can join (this direction >>)")

                    val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
                    val rightJVar = omitQuestionMark(op2) + "_ID"
                    jDF = jDF.join(df2, jDF.col(leftJVar).equalTo(df2.col(rightJVar)))

                    seenDF.addOne((op2, "ID"))


                } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
                    logger.info("...we can join (this direction >>)")

                    val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
                    val rightJVar = omitQuestionMark(op2) + "_ID"
                    jDF = df1.join(jDF, df1.col(leftJVar).equalTo(jDF.col(rightJVar)))

                    seenDF.addOne((op1, jVal))

                } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {
                    logger.info("...no join possible -> GOING TO THE QUEUE")
                    pendingJoins.enqueue((op1, (op2, jVal)))
                }
            }
        }

        while (pendingJoins.nonEmpty) {
            logger.info("ENTERED QUEUED AREA: " + pendingJoins)
            val dfs_only = seenDF.map(_._1)

            val e = pendingJoins.head

            val op1 = e._1
            val op2 = e._2._1
            val jVal = e._2._2

            val njVal = get_NS_predicate(jVal)
            val ns = prefixes(njVal._1)

            logger.info(s"-> Joining ($op1 $join $op2) using $jVal...")

            val df1 = star_df(op1)
            val df2 = star_df(op2)

            if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
                val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
                val rightJVar = omitQuestionMark(op2) + "_ID"
                jDF = jDF.join(df2, jDF.col(leftJVar).equalTo(df2.col(rightJVar))) // deep-left

                seenDF.addOne((op2, "ID"))
            } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
                val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
                val rightJVar = omitQuestionMark(op2) + "_ID"
                jDF = jDF.join(df1, df1.col(leftJVar).equalTo(jDF.col(rightJVar))) // deep-left

                seenDF.addOne((op1, jVal))
            } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {
                pendingJoins.enqueue((op1, (op2, jVal)))
            }

            pendingJoins = pendingJoins.tail
        }

        jDF
    }

    def joinReordered(joins: ArrayListMultimap[String, (String, String)], prefixes: Map[String, String], star_df: Map[String, DataFrame],
                      startingJoin: (String, (String, String)), starWeights: Map[String, Double]): DataFrame = {
        import scala.jdk.CollectionConverters._
        import scala.collection.mutable.ListBuffer

        val seenDF : ListBuffer[(String, String)] = ListBuffer()
        val joinSymbol = " x "
        var jDF : DataFrame = null

        val op1 = startingJoin._1
        val op2 = startingJoin._2._1
        val jVal = startingJoin._2._2
        val njVal = get_NS_predicate(jVal)
        val ns = prefixes(njVal._1)
        val df1 = star_df(op1)
        val df2 = star_df(op2)

        logger.info(s"-> DOING FIRST JOIN ($op1 $joinSymbol $op2) USING $jVal (namespace: $ns)")

        seenDF.addOne((op1, jVal))
        seenDF.addOne((op2, "ID")) // TODO: implement join var in the right side too

        // Join level 1
        val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
        val rightJVar = omitQuestionMark(op2) + "_ID"
        jDF = df1.join(df2, df1.col(leftJVar).equalTo(df2(rightJVar)))

        joins.remove(startingJoin._1, (startingJoin._2._1, startingJoin._2._2))

        logger.info("...done!")

        var joinsMap : Map[(String, String), String] = Map()
        for (jj <- joins.entries().asScala) {
            joinsMap += (jj.getKey, jj.getValue._1) -> jj.getValue._2
        }
        val seenDF1 : mutable.Set[(String, String)] = mutable.Set()
        for (s <- seenDF) {
            seenDF1 += s
        }

        logger.info("joinsMap: " + joinsMap)
        while(joinsMap.asJava.size() > 0) {

            val dfs_only = seenDF.map(_._1)
            logger.info(s"-> Looking for join(s) that join(s) with: $dfs_only")

            var joinable : Map[(String, String), String] = Map.empty // or Map()

            val j = joinsMap.iterator
            while ({j.hasNext}) {
                val entry = j.next

                val op1 = entry._1._1
                val op2 = entry._1._2
                val jVal = entry._2

                val njVal = get_NS_predicate(jVal)
                val ns = prefixes(njVal._1)

                if (dfs_only.contains(op1) || dfs_only.contains(op2)) {
                    joinable += ((op1, op2) -> jVal)
                    joinsMap -= ((op1, op2))
                }
            }

            logger.info("Found those: " + joinable)

            var weighedJoins : Map[(String, (String, String), String), Double] = Map()
            for(jj <- joinable) {
                val op1 = jj._1._1
                val op2 = jj._1._2
                val jVal = jj._2

                if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
                    logger.info(s"...getting weight of join variable $op2")

                    weighedJoins += (op1, (op2, jVal), "op2") -> starWeights(op2)

                } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
                    logger.info(s"...getting weight of join variable $op1")

                    weighedJoins += (op1, (op2, jVal), "op1") -> starWeights(op1)
                }
            }

            // Sort joins by their weight on the joining side
            logger.info(s"weighedJoins: $weighedJoins")

            val sortedWeighedJoins = ListMap(weighedJoins.toSeq.sortWith(_._2 > _._2) : _*)

            logger.info(s"sortedWeighedJoins: $sortedWeighedJoins")

            for(s <- sortedWeighedJoins) {
                val op1 = s._1._1
                val op2 = s._1._2._1
                val jVal = s._1._2._2
                val njVal = get_NS_predicate(jVal)
                val ns = prefixes(njVal._1)
                val joinSide = s._1._3

                val df1 = star_df(op1)
                val df2 = star_df(op2)

                logger.info(s"---- $op1 -- $op2 -- $joinSide -- $jVal")

                if (joinSide.equals("op2")) {
                    logger.info("...we can join (this direction >>) ")

                    val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
                    val rightJVar = omitQuestionMark(op2) + "_ID"

                    logger.info(s"$leftJVar XXX $rightJVar")
                    jDF = jDF.join(df2, jDF.col(leftJVar).equalTo(df2.col(rightJVar)))

                    seenDF.addOne((op2, "ID"))
                } else if (joinSide.equals("op1")) {
                    logger.info("...we can join (this direction <<) ")

                    val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
                    val rightJVar = omitQuestionMark(op2) + "_ID"
                    jDF = df1.join(jDF, df1.col(leftJVar).equalTo(jDF.col(rightJVar)))

                    seenDF.addOne((op1, jVal))
                }
            }
            logger.info(s"-> Fully joined: $seenDF \n")
        }

        jDF
    }

    def project(jDF: Any, columnNames: Seq[String], distinct: Boolean): DataFrame = {
        if (!distinct) {
            jDF.asInstanceOf[DataFrame].select(columnNames.head, columnNames.tail : _*)
        } else {
            jDF.asInstanceOf[DataFrame].select(columnNames.head, columnNames.tail : _*).distinct()
        }
    }

    def schemaOf(jDF: DataFrame): Unit = {
        jDF.printSchema()
    }

    def count(jDF: DataFrame): Long = {
        jDF.count()
    }

    def orderBy(jDF: Any, direction: String, variable: String): DataFrame = {
        logger.info("ORDERING...")

        if (direction == "-1") {
            jDF.asInstanceOf[DataFrame].orderBy(asc(variable))
        } else { // TODO: assuming the other case is automatically -1 IFNOT change to "else if (direction == "-2") {"
            jDF.asInstanceOf[DataFrame].orderBy(desc(variable))
        }
    }

    def groupBy(jDF: Any, groupBys: (ListBuffer[String], mutable.Set[(String, String)])): DataFrame = {

        val groupByVars = groupBys._1
        val aggregationFunctions = groupBys._2

        val cols : ListBuffer[Column] = ListBuffer()
        for (gbv <- groupByVars) {
            cols += col(gbv)
        }
        logger.info("aggregationFunctions: " + aggregationFunctions)

        var aggSet : mutable.Set[(String, String)] = mutable.Set()
        for (af <- aggregationFunctions) {
            aggSet += ((af._1, af._2))
        }
        val aa = aggSet.toList
        val newJDF : DataFrame = jDF.asInstanceOf[DataFrame].groupBy(cols.toList: _*).agg(aa.head, aa.tail : _*)

        newJDF.printSchema()

        newJDF
    }

    def limit(jDF: Any, limitValue: Int) : DataFrame = jDF.asInstanceOf[DataFrame].limit(limitValue)

    def show(jDF: Any): Unit = {
        val columns = ArrayBuffer[String]()
        // jDF.asInstanceOf[DataFrame].show
        val df = jDF.asInstanceOf[DataFrame]
        // df.printSchema()
        val schema = df.schema
        for (col <- schema)
            columns += col.name

        println(columns.mkString(","))
        df.take(20).foreach(x => println(x))

        println(s"Number of results: ${jDF.asInstanceOf[DataFrame].count()}")
    }

    def run(jDF: Any): Unit = {
        this.show(jDF)
    }
}
