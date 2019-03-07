
package net.sansa_stack.datalake.spark

import org.apache.jena.query.{QueryExecutionFactory, QueryFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.util.FileManager

import scala.collection.mutable

import java.io.ByteArrayInputStream

import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection.mutable.{HashMap, Set}

/**
  * Created by mmami on 30.01.17.
  */
class Mapper (mappingsFile: String) {

    def findDataSources(
                           stars: mutable.HashMap[
                                String,
                                mutable.Set[(String, String)]
                            ] with mutable.MultiMap[
                                String,
                                (String, String)
                            ],
                            configFile: String
                        ) :
                        // returns
                        mutable.Set[(String,
                                        Set[(mutable.HashMap[String, String], String, String)],
                                        HashMap[String, (Map[String, String], String)]
                                    )] = {

        val starSources :
            mutable.Set[(
                String, // Star core
                mutable.Set[(mutable.HashMap[String, String], String, String)], // A set of data sources relevant to the Star (pred_attr, src, srcType)
                mutable.HashMap[String, (Map[String, String], String)] // A set of options of each relevant data source
            )] = mutable.Set()

        var count = 0

        var starDatasourceTypeMap : Map[String, String] = Map()

        for (s <-stars) {
            val subject = s._1 // core of the star
            val predicates_objects = s._2

            println(s"\n- Going to find datasources relevant to $subject...")
            val ds = findDataSource(predicates_objects) // One or more relevant data sources
            count = count + 1

            // Options of relevant sources of one star
            val optionsentityPerStar : mutable.HashMap[String, (Map[String, String], String)] = new HashMap()

            // Iterate through the relevant data sources to get options
            // One star can have many relevant sources (containing its predicates)
            for (d <- ds) {
                val src = d._2

                var configJSON = ""
                if(!configFile.startsWith("hdfs://")) {
                        var configs = scala.io.Source.fromFile(configFile)
                        configJSON = try configs.mkString finally configs.close()
                } else {
                        val host_port = configFile.split("/")(2).split(":")
                        val host = host_port(0)
                        val port = host_port(1)
                        val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://" + host + ":" + port + "/"), new org.apache.hadoop.conf.Configuration())
                        val path = new org.apache.hadoop.fs.Path(configFile)
                        val stream = hdfs.open(path)
                        def readLines = scala.io.Source.fromInputStream(stream)
                        configJSON = readLines.mkString
                }

                case class ConfigObject(source : String, options: Map[String, String], entity : String)

                implicit val userReads : Reads[ConfigObject] = (
                    (__ \ 'source).read[String] and
                    (__ \ 'options).read[Map[String, String]] and
                    (__ \ 'entity).read[String]
                ) (ConfigObject)

                val sources = (Json.parse(configJSON) \ "sources").as[Seq[ConfigObject]]

                for (s <- sources) {
                    if (s.source == src) {
                        val source = s.source
                        val options = s.options
                        val entity = s.entity

                        optionsentityPerStar.put(source, (options, entity))
                    }
                }
            }

            starSources.add((subject, ds, optionsentityPerStar))
        }

        // return: subject (star core), list of (data source, options)
        return starSources
    }

    private def findDataSource(predicates_objects: Set[(String, String)]) : Set[(HashMap[String, String], String, String)] = {
        var listOfPredicatesForQuery = ""
        val listOfPredicates : Set[String] = Set()
        val returnedSources : Set[(HashMap[String, String], String, String)] = Set()

        var temp = 0

        println("...with the (Predicate,Object) pairs: " + predicates_objects)

        for(v <- predicates_objects) {
            val predicate = v._1

            if(predicate == "rdf:type" || predicate == "a") {
                println("...of class: " + v._2)
                listOfPredicatesForQuery += "?mp rr:subjectMap ?sm . ?sm rr:class " + v._2 + " . "

            } else {
                listOfPredicatesForQuery += "?mp rr:predicateObjectMap ?pom" + temp + " . " +
                    "?pom" + temp + " rr:predicate " + predicate + " . " +
                    "?pom" + temp + " rr:objectMap ?om" + temp + " . "

                listOfPredicates.add(predicate)
                temp +=1

            }
        }

        val queryString = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#>" +
                            "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
                            "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
                            "PREFIX nosql: <http://purl.org/db/nosql#>" +
            "SELECT distinct ?src ?type WHERE {" +
                "?mp rml:logicalSource ?ls . " +
                "?ls rml:source ?src . " +
                "?ls nosql:store ?type . " +
                listOfPredicatesForQuery +
            "}"

        println("...for this, the following query will be executed: " + queryString + " on " + mappingsFile)
        val query = QueryFactory.create(queryString)

        var mappingsString = ""
        if(!mappingsFile.startsWith("hdfs://")) {
                var mappings = scala.io.Source.fromFile(mappingsFile)
                mappingsString = try mappings.mkString finally mappings.close()
        } else {
                val host_port = mappingsFile.split("/")(2).split(":")
                val host = host_port(0)
                val port = host_port(1)
                val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://" + host + ":" + port + "/"), new org.apache.hadoop.conf.Configuration())
                val path = new org.apache.hadoop.fs.Path(mappingsFile)
                val stream = hdfs.open(path)
                def readLines = scala.io.Source.fromInputStream(stream)
                mappingsString = readLines.mkString
        }

        val in = new ByteArrayInputStream(mappingsString.getBytes)

        if (in == null) {
            throw new IllegalArgumentException("ERROR: File: " + queryString + " not found")
        }

        val model = ModelFactory.createDefaultModel()
        model.read(in, null, "TURTLE")

        // Execute the query and obtain results
        val qe = QueryExecutionFactory.create(query, model)
        val results = qe.execSelect()

        while(results.hasNext) { // only one result expected (for the moment)
            val soln = results.nextSolution()
            val src = soln.get("src").toString
            val srcType = soln.get("type").toString

            println(">>> Relevant source detected [" + src + "] of type [" + srcType + "]")

            val pred_attr: HashMap[String, String] = HashMap()

            for (p <- listOfPredicates) {

                val getAttributeOfPredicate = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#> " +
                    "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
                    "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
                    "SELECT ?r WHERE {" +
                    "?mp rml:logicalSource ?ls . " +
                    "?ls rml:source \"" + src + "\" . " +
                    "?mp rr:predicateObjectMap ?pom . " +
                    "?pom rr:predicate  " + p + " . " +
                    "?pom rr:objectMap ?om . " +
                    "?om rml:reference ?r . " +
                    "}"


                val query1 = QueryFactory.create(getAttributeOfPredicate)
                val qe1 = QueryExecutionFactory.create(query1, model)
                val results1 = qe1.execSelect()
                while (results1.hasNext) {
                    val soln1 = results1.nextSolution()
                    val attr = soln1.get("r").toString
                    pred_attr.put(p, attr)
                }
            }
            returnedSources.add((pred_attr, src, srcType))
        }

        qe.close() // Important: free up resources used running the query

        returnedSources
    }
}
