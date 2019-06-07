package net.sansa_stack.datalake.spark

import java.util

import com.google.common.collect.ArrayListMultimap
import com.typesafe.scalalogging.Logger
import net.sansa_stack.datalake.spark.utils.Helpers._
import play.api.libs.functional.syntax._
import play.api.libs.json.{__, Json, Reads}
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer, MultiMap, Set}

/**
  * Created by mmami on 06.07.17.
  */
class Planner(stars: HashMap[String, Set[(String, String)]] with MultiMap[String, (String, String)]) {

    val logger = Logger("SANSA-DataLake")

    def getNeededPredicates(star_predicate_var: mutable.HashMap[(String, String), String], joins: ArrayListMultimap[String, (String, String)],
                            select_vars: util.List[String]) : (Set[String], Set[(String, String)]) = {

        logger.info("star_predicate_var: " + star_predicate_var)
        val predicates : Set[String] = Set.empty
        val predicatesForSelect : Set[(String, String)] = Set.empty

        val join_left_vars = joins.keySet()
        val join_right_vars = joins.values().asScala.map(x => x._1).toSet // asScala, converts Java Collection to Scala Collection

        val join_left_right_vars = join_right_vars.union(join_left_vars.asScala)

        logger.info("--> Left & right join operands: " + join_left_right_vars)

        for (t <- star_predicate_var) {
            val s_p = t._1
            val o = t._2

            val occurrences = star_predicate_var groupBy ( _._2 ) mapValues( _.size ) // To capture variables (objects) used in more than one predicate

            if (select_vars.contains(o.replace("?", "")) || join_left_vars.contains(o) || join_right_vars.contains(o) || occurrences(o) > 1) {
                predicates.add(s_p._2)
            }


            if (select_vars.contains(o.replace("?", ""))) {
                predicatesForSelect.add(s_p)
            }

        }

        (predicates, predicatesForSelect)
    }

    def generateJoinPlan: (ArrayListMultimap[String, (String, String)], Set[String], Set[String], Map[(String, String), String]) = {

        val keys = stars.keySet.toSeq
        logger.info("Stars: " + keys.toString())
        val joins : ArrayListMultimap[String, (String, String)] = ArrayListMultimap.create[String, (String, String)]()
        var joinPairs : Map[(String, String), String] = Map.empty

        val joinedToFlag : Set[String] = Set()
        val joinedFromFlag : Set[String] = Set()

        for(i <- keys.indices) {
            var currentSubject = keys(i)
            var valueSet = stars(currentSubject)
            for(p_o <- valueSet) {
                var o = p_o._2
                if (keys.contains(o)) { // A previous star of o
                    var p = p_o._1
                    joins.put(currentSubject, (o, p))
                    joinPairs += (omitQuestionMark(currentSubject), omitQuestionMark(o)) -> p
                    joinedToFlag.add(o)
                    joinedFromFlag.add(currentSubject)
                }
            }
        }

        (joins, joinedToFlag, joinedFromFlag, joinPairs)
    }

    def reorder(joins: ArrayListMultimap[String, (String, String)], starDataTypesMap: Map[String, mutable.Set[String]],
                starNbrFilters: Map[String, Integer], starWeights: Map[String, Double], configFile: String): ListMap[(String, String), Double] = {

        logger.info("...REORDERING JOINS, if needed...")

        var joinsToReorder : ListBuffer[(String, String)] = ListBuffer()

        for (j <- joins.entries.asScala) {
            joinsToReorder += ((j.getKey, j.getValue._1))
        }

        val scoredJoins = getScoredJoins(joins, starWeights)

        val sortedScoredJoins = ListMap(scoredJoins.toSeq.sortWith(_._2 > _._2) : _*)

        sortedScoredJoins
    }

    def getScoredJoins(joins : ArrayListMultimap[String, (String, String)], scores: Map[String, Double]): Map[(String, String), Double] = {
        var scoredJoins : Map[(String, String), Double] = Map()

        for (j <- joins.entries.asScala)
            scoredJoins += (j.getKey, j.getValue._1) -> (scores(j.getKey) + scores(j.getValue._1))

        scoredJoins
    }

    def sortStarsByWeight(starDataTypesMap: Map[String, mutable.Set[String]], filters: Map[String, Integer], configFile: String): Map[String, Double] = {
        var configJSON = ""
        if (configFile.startsWith("hdfs://")) {
            val host_port = configFile.split("/")(2).split(":")
            val host = host_port(0)
            val port = host_port(1)
            val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://" + host + ":" + port + "/"), new org.apache.hadoop.conf.Configuration())
            val path = new org.apache.hadoop.fs.Path(configFile)
            val stream = hdfs.open(path)

            def readLines = scala.io.Source.fromInputStream(stream)

            configJSON = readLines.mkString
        } else if (configFile.startsWith("s3")) { // E.g., s3://sansa-datalake/config
            val bucket_key = configFile.replace("s3://", "").split("/")
            val bucket = bucket_key.apply(0) // apply(x) = (x)
            val key = if (bucket_key.length > 2) bucket_key.slice(1, bucket_key.length).mkString("/") else bucket_key(1) // Case of folder

            import com.amazonaws.services.s3.AmazonS3Client
            import com.amazonaws.services.s3.model.GetObjectRequest
            import java.io.BufferedReader
            import java.io.InputStreamReader

            val s3 = new AmazonS3Client

            val s3object = s3.getObject(new GetObjectRequest(bucket, key))

            val reader: BufferedReader = new BufferedReader(new InputStreamReader(s3object.getObjectContent))
            val lines = new ArrayBuffer[String]()
            var line: String = null
            while ({line = reader.readLine; line != null}) {
                lines.asJava.add(line)
            }
            reader.close()

            configJSON = lines.mkString("\n")
        } else {
            val configs = scala.io.Source.fromFile(configFile)
            configJSON = try configs.mkString finally configs.close()
        }

        case class ConfigObject(datasource: String, weight: Double)

        implicit val userReads: Reads[ConfigObject] = (
            (__ \ 'datasource).read[String] and
                (__ \ 'weight).read[Double]
            )(ConfigObject)

        val weights = (Json.parse(configJSON) \ "weights").as[Seq[ConfigObject]]

        var scoresByDatasource : Map[String, Double] = Map()
        for (w <- weights) {
            scoresByDatasource += w.datasource -> w.weight
        }

        logger.info(s"- We use the following scores of the datasource types: $scoresByDatasource \n")

        val scores = starScores(starDataTypesMap, scoresByDatasource, filters)

        scores
    }

    def starScores(starDataTypesMap: Map[String, mutable.Set[String]], weightsByDatasource: Map[String, Double], filters: Map[String, Integer]): Map[String, Double] = {
        var scores : Map[String, Double] = Map()

        var datasourceTypeWeight = 0.0 // Coucou!

        for (s <- starDataTypesMap) {
            val star = s._1 // eg. ?r
            val datasourceTypeURI_s = s._2 // eg. http://purl.org/db/nosql#cassandra

            val nbrFilters = filters(star).toInt

            if (datasourceTypeURI_s.size == 1) { // only one relevant datasource
                val datasourceType = datasourceTypeURI_s.head.split("#")(1) // eg. cassandra

                if (nbrFilters > 0) {
                    datasourceTypeWeight = weightsByDatasource(datasourceType) + 1
                } else {
                    datasourceTypeWeight = weightsByDatasource(datasourceType)
                }
                // Add  the number of filters to the score of the star
            }
            // else, we keep 0, as we are assuming if there are more than 1 data sources, queryig & union-ing them would be expensive
            scores += (star -> datasourceTypeWeight)
        }

        scores
    }
}
