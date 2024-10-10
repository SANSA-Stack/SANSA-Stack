package net.sansa_stack.datalake.spark

import java.io.ByteArrayInputStream

import com.typesafe.scalalogging.Logger
import net.sansa_stack.datalake.spark.utils.Helpers
import org.apache.jena.query.{QueryExecutionFactory, QueryFactory}
import org.apache.jena.rdf.model.ModelFactory
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class Mapper (mappingsFile: String) {

    val logger = Logger("SANSA-DataLake")

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
                                        mutable.Set[(mutable.HashMap[String, String], String, String, mutable.HashMap[String, (String, Boolean)])],
                                        mutable.HashMap[String, (Map[String, String], String)]
                                    )] = {

        val starSources :
            mutable.Set[(
                String, // Star core
                mutable.Set[(mutable.HashMap[String, String], String, String, mutable.HashMap[String, (String, Boolean)])], // A set of data sources relevant to the Star (pred_attr, src, srcType)
                mutable.HashMap[String, (Map[String, String], String)] // A set of options of each relevant data source
            )] = mutable.Set()

        var count = 0

        for (s <- stars) {
            val subject = s._1 // core of the star
            val predicates_objects = s._2

            logger.info(s"\n- Going to find datasources relevant to $subject...")
            val ds = findDataSource(predicates_objects) // One or more relevant data sources
            count = count + 1

            // Options of relevant sources of one star
            val optionsentityPerStar : mutable.HashMap[String, (Map[String, String], String)] = new mutable.HashMap()

            // Iterate through the relevant data sources to get options
            // One star can have many relevant sources (containing its predicates)
            for (d <- ds) {
                val src = d._2

                val configJSON = Helpers.readFileFromPath(configFile)

                case class ConfigObject(source: String, options: Map[String, String], entity: String)

                implicit val userReads: Reads[ConfigObject] = (
                    (__ \ Symbol("source")).read[String] and
                    (__ \ Symbol("options")).read[Map[String, String]] and
                    (__ \ Symbol("entity")).read[String]
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
        starSources
    }

    private def findDataSource(predicates_objects: mutable.Set[(String, String)]) : mutable.Set[(mutable.HashMap[String, String], String, String, mutable.HashMap[String, (String, Boolean)])] = {
        var listOfPredicatesForQuery = ""
        val listOfPredicates : mutable.Set[String] = mutable.Set()
        val returnedSources : mutable.Set[(mutable.HashMap[String, String], String, String, mutable.HashMap[String, (String, Boolean)])] = mutable.Set()

        var temp = 0

        logger.info("...with the (Predicate, Object) pairs: " + predicates_objects)

        for (v <- predicates_objects) {
            val predicate = v._1

            if (predicate == "rdf:type" || predicate == "a") {
                logger.info("...of class: " + v._2)
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

        logger.info("...for this, the following query will be executed: " + queryString + " on " + mappingsFile)
        val query = QueryFactory.create(queryString)

        val mappingsString = Helpers.readFileFromPath(mappingsFile)

        val in = new ByteArrayInputStream(mappingsString.getBytes)

        if (in == null) {
            throw new IllegalArgumentException("ERROR: File: " + mappingsString + " not found")
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

            logger.info(">>> Relevant source detected [" + src + "] of type [" + srcType + "]") // considering only first one src

            val predicate_attribute: mutable.HashMap[String, String] = mutable.HashMap()
            val predicate_transformations: mutable.HashMap[String, (String, Boolean)] = mutable.HashMap()

            // We will look for predicate transformations (subject transformations later on)
            for (p <- listOfPredicates) {

                val getAttributeOfPredicate = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#> " +
                    "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
                    "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
                    "SELECT ?om ?r ?id WHERE {" +
                    "?mp rml:logicalSource ?ls . " +
                    "?ls rml:source \"" + src + "\" . " +
                    "?mp rr:subjectMap ?sm . " +
                    "?sm rr:template ?id . " +
                    "?mp rr:predicateObjectMap ?pom . " +
                    "?pom rr:predicate  " + p + " . " +
                    "?pom rr:objectMap ?om . " +
                    "OPTIONAL {?om rml:reference ?r} . " +
                    "}"

                val query1 = QueryFactory.create(getAttributeOfPredicate)
                val qe1 = QueryExecutionFactory.create(query1, model)
                val results1 = qe1.execSelect()

                while (results1.hasNext) {
                    val soln1 = results1.nextSolution()
                    val om = soln1.getResource("om")

                    var fn = ""
                    var attr = ""
                    var trans : ListBuffer[String] = ListBuffer()

                    if (om.getURI != null) { // the case of FunctionMap

                        // Get function
                        val queryString = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#> " +
                          "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
                          "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
                          "PREFIX edm: <http://www.europeana.eu/schemas/edm/>" +
                          "PREFIX fnml: <http://semweb.mmlab.be/ns/fnml#>" +
                          "PREFIX fno: <http://w3id.org/function/ontology#>" +
                          "PREFIX grel: <http://users.ugent.be/~bjdmeest/function/grel.ttl#>" +
                          "SELECT ?fn ?ref WHERE {" +
                          "<#" + om.getLocalName + "> fnml:functionValue ?fv . " +
                          "?fv rml:logicalSource \"" + src + "\" . " +
                          "?fv rr:predicateObjectMap ?pom . " +
                          "?pom rr:predicate  fno:executes . " +
                          "?pom rr:objectMap ?om . " +
                          "?om rr:constant ?fn . " +
                          "?fv rr:predicateObjectMap ?pom1 . " +  // we don't use multiple ?pom's coz we don't know how
                          "?pom1 rr:predicate  ?param . " +       // many params we have, eg. toUpperCase only 1 param.
                          "?pom1 rr:objectMap ?om1 . " +          // so, 1st ref is the attribute, rest are fnt params
                          "?om1 rr:reference ?ref . " +
                          "}"

                        val query2 = QueryFactory.create(queryString)
                        val qe2 = QueryExecutionFactory.create(query2, model)
                        val results2 = qe2.execSelect()
                        while (results2.hasNext) {
                            val soln2 = results2.nextSolution()

                            fn = soln2.get("fn").toString
                            attr = soln2.get("ref").toString // Used also for pred_attr.put()

                            trans += fn
                            trans += attr
                        }
                        trans = trans.distinct // to omit duplicates, in this case the function URI e.g. _:greaterThan

                        logger.info(s"Transformations for predicate $p (attr: $attr): $trans")
                        predicate_transformations.put(p, (trans.mkString(" "), false))

                    } else {
                        try {
                            attr = soln1.get("r").toString
                        } catch {
                        case _: NullPointerException => println("ERROR: Relevant source detected but cannot " +
                          "be read due to mappings issues. For example, are you using `rr:parentTriplesMap` instead of `rml:reference`?")
                            System.exit(1)
                        }
                    }

                    predicate_attribute.put(p, attr)
                }
            }

            // We will look for subject transformations
            val getAttributeOfPredicate = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#> " +
              "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
              "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
              "SELECT ?fn ?id WHERE {" +
              "?mp rml:logicalSource ?ls . " +
              "?ls rml:source \"" + src + "\" . " +
              "?mp rr:subjectMap ?sm . " +
              "?sm rr:objectMap ?fn ." +
              "}"

            val query1 = QueryFactory.create(getAttributeOfPredicate)
            val qe1 = QueryExecutionFactory.create(query1, model)
            val results1 = qe1.execSelect()

            while (results1.hasNext) {
                val soln1 = results1.nextSolution()
                val fnMap = soln1.getResource("fn")

                var fn = ""
                var attr = ""
                var trans: ListBuffer[String] = ListBuffer()

                if (fnMap != null) { // the case of FunctionMap
                    // Get function
                    val queryString = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#> " +
                      "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
                      "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
                      "PREFIX edm: <http://www.europeana.eu/schemas/edm/>" +
                      "PREFIX fnml: <http://semweb.mmlab.be/ns/fnml#>" +
                      "PREFIX fno: <http://w3id.org/function/ontology#>" +
                      "PREFIX grel: <http://users.ugent.be/~bjdmeest/function/grel.ttl#>" +
                      "SELECT ?fn ?ref WHERE {" +
                      "<#" + fnMap.getLocalName + "> fnml:functionValue ?fv . " +
                      "?fv rml:logicalSource \"" + src + "\" . " +
                      "?fv rr:predicateObjectMap ?pom . " +
                      "?pom rr:predicate  fno:executes . " +
                      "?pom rr:objectMap ?om . " +
                      "?om rr:constant ?fn . " +
                      "?fv rr:predicateObjectMap ?pom1 . " + // we don't use multiple ?pom's coz we don't know how
                      "?pom1 rr:predicate  ?param . " + // many params we have, eg. toUpperCase only 1 param.
                      "?pom1 rr:objectMap ?om1 . " + // so, 1st ref is the attribute, rest are fnt params
                      "?om1 rr:reference ?ref . " +
                      "}"

                    // val id = soln1.get("id").toString.stripSuffix("}").split("\\{")(1) // get 'id' from 'url{id}'

                    val query2 = QueryFactory.create(queryString)
                    val qe2 = QueryExecutionFactory.create(query2, model)
                    val results2 = qe2.execSelect()
                    while (results2.hasNext) {
                        val soln2 = results2.nextSolution()

                        fn = soln2.get("fn").toString
                        attr = soln2.get("ref").toString // Used also for pred_attr.put()

                        trans += fn
                        trans += attr
                    }
                    trans = trans.distinct // to omit duplicates, in this case the function URI e.g. _:greaterThan

                    logger.info(s"Transformations for subject/ID ($attr): $trans")
                    predicate_transformations.put("ID", (trans.mkString(" "), true))
                }
            }

            returnedSources.add((predicate_attribute, src, srcType, predicate_transformations))
        }

        qe.close() // Important: free up resources used running the query

        returnedSources
    }
}
