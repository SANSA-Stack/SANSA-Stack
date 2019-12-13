package net.sansa_stack.datalake.spark.utils

import java.io.ByteArrayInputStream
import java.util

import com.google.common.collect.ArrayListMultimap
import com.typesafe.scalalogging.Logger
import org.apache.jena.query.{QueryExecutionFactory, QueryFactory}
import org.apache.jena.rdf.model.ModelFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by mmami on 26.07.17.
  */
object Helpers {

    val logger = Logger("SANSA-DataLake")

    def invertMap(prolog: util.Map[String, String]): Map[String, String] = {
        var star_df : Map[String, String] = Map.empty

        val keys = prolog.keySet()
        val it = keys.iterator()
        while(it.hasNext) {
            val key : String = it.next()
            star_df += (prolog.get(key) -> key)
        }

        star_df
    }

    def omitQuestionMark(str: String): String = str.replace("?", "")


    def omitNamespace(URI: String): String = {
        val URIBits = URI.replace("<", "").replace(">", "").replace("#", "/").split("/")
        URIBits(URIBits.length-1)
    }

    def getNamespaceFromURI(URI: String): String = {
        "" // TODO: create
    }

    def get_NS_predicate(predicateURI: String): (String, String) = {

        val url = predicateURI.replace("<", "").replace(">", "")
        val URIBits = url.split("/")

        var pred = ""
        if(predicateURI.contains("#")) {
            pred = URIBits(URIBits.length-1).split("#")(1) // like: http://www.w3.org/2000/01/[rdf-schema#label]
        } else {
            pred = URIBits(URIBits.length-1)
        }

        val ns = url.replace(pred, "")

        (ns, pred)
    }

    def getTypeFromURI(typeURI: String) : String = {
        val dataType = typeURI.split("#") // from nosql ns

        val rtrn = dataType(dataType.length-1)

        rtrn
    }

    def getSelectColumnsFromSet(pred_attr: mutable.HashMap[String, String],
                                star: String,
                                prefixes: Map[String, String],
                                select: util.List[String],
                                star_predicate_var: mutable.HashMap[(String, String), String],
                                neededPredicates: mutable.Set[String],
                                filters: ArrayListMultimap[String, (String, String)]
                               ): String = {

        var columns = ""
        var i = 0

        for (v <- pred_attr) {
            val attr = v._2
            val ns_predicate = Helpers.get_NS_predicate(v._1)

            val ns_predicate_bits = ns_predicate
            val NS = ns_predicate_bits._1
            val predicate = ns_predicate_bits._2

            val objVar = star_predicate_var(("?" + star, "<" + NS + predicate + ">"))

            logger.info("-> Variable: " + objVar + " exists in WHERE, is it in SELECT? " + select.contains(objVar.replace("?", "")))

            if (neededPredicates.contains(v._1)) {
                val c = " `" + attr + "` AS `" + star + "_" + predicate + "_" + prefixes(NS) + "`"

                if (i == 0) columns += c else columns += "," + c
                i += 1
            }

            if (filters.keySet().contains(objVar)) {
                val c = " `" + attr + "` AS `" + star + "_" + predicate + "_" + prefixes(NS) + "`"

                if (!columns.contains(c)) { // if the column has already been added from the SELECT predicates
                    if (i == 0) columns += c else columns += "," + c
                    i += 1
                }

            }
        }

        columns
    }

    def getSelectColumnsFromSet(pred_attr: mutable.HashMap[String, String],
                                star: String,
                                prefixes: Map[String, String],
                                select: util.List[String],
                                star_predicate_var: mutable.HashMap[(String, String), String],
                                neededPredicates: mutable.Set[String]
        ): String = {

        var columns = ""
        var i = 0

        for (v <- pred_attr) {
            val attr = v._2
            val ns_predicate = Helpers.get_NS_predicate(v._1)

            val ns_predicate_bits = ns_predicate
            val NS = ns_predicate_bits._1
            val predicate = ns_predicate_bits._2

            val objVar = star_predicate_var(("?" + star, "<" + NS + predicate + ">"))

            logger.info("-> Variable: " + objVar + " exists in WHERE, is it in SELECT? " + select.contains(objVar.replace("?", "")))

            if (neededPredicates.contains(v._1)) {
                val c = attr + " AS `" + star + "_" + predicate + "_" + prefixes(NS) + "`"
                if (i == 0) columns += c else columns += "," + c
                i += 1
            }
        }

        columns
    }

    def getID(sourcePath: String, mappingsFile: String): String = {

        var getID = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#>" +
            "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
            "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
            "SELECT ?t WHERE {" +
                "?mp rml:logicalSource ?ls . " +
                "?ls rml:source \"" + sourcePath + "\" . " +
                "?mp rr:subjectMap ?sm . " +
                "?sm rr:template ?t " +
            "}"

        var mappingsString = ""
        if (mappingsFile.startsWith("hdfs://")) {
            val host_port = mappingsFile.split("/")(2).split(":")
            val host = host_port(0)
            val port = host_port(1)
            val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://" + host + ":" + port + "/"), new org.apache.hadoop.conf.Configuration())
            val path = new org.apache.hadoop.fs.Path(mappingsFile)
            val stream = hdfs.open(path)

            def readLines = scala.io.Source.fromInputStream(stream)

            mappingsString = readLines.mkString
        } else if (mappingsFile.startsWith("s3")) { // E.g., s3://sansa-datalake/config
            val bucket_key = mappingsFile.replace("s3://", "").split("/")
            val bucket = bucket_key.apply(0) // apply(x) = (x)
            val key = if (bucket_key.length > 2) bucket_key.slice(1, bucket_key.length).mkString("/") else bucket_key(1) // Case of folder

            import com.amazonaws.services.s3.AmazonS3Client
            import com.amazonaws.services.s3.model.GetObjectRequest
            import java.io.BufferedReader
            import java.io.InputStreamReader
            import scala.collection.JavaConverters._

            val s3 = new AmazonS3Client

            val s3object = s3.getObject(new GetObjectRequest(bucket, key))

            val reader: BufferedReader = new BufferedReader(new InputStreamReader(s3object.getObjectContent))
            val lines = new ArrayBuffer[String]()
            var line: String = null
            while ({line = reader.readLine; line != null}) {
                lines.asJava.add(line)
            }
            reader.close()

            mappingsString = lines.mkString("\n")
        } else {
            var mappings = scala.io.Source.fromFile(mappingsFile)
            mappingsString = try mappings.mkString finally mappings.close()
        }

        val in = new ByteArrayInputStream(mappingsString.getBytes)

        val model = ModelFactory.createDefaultModel()
        model.read(in, null, "TURTLE")

        var id = ""

        val query1 = QueryFactory.create(getID)
        val qe1 = QueryExecutionFactory.create(query1, model)
        val results1 = qe1.execSelect()
        while (results1.hasNext) {
            val soln1 = results1.nextSolution()
            val template = soln1.get("t").toString

            val templateBits = template.split("/")
            id = templateBits(templateBits.length-1).replace("{", "").replace("}", "")
        }

        id
    }

    def makeMongoURI(uri: String, database: String, collection: String, options: String): String = {
        if (options == null) {
            s"mongodb://$uri/$database.$collection"
        } else {
            s"mongodb://$uri/$database.$collection?$options"
        }
        // mongodb://db1.example.net,db2.example.net:27002,db3.example.net:27003/?db_name&replicaSet=YourReplicaSetName
        // mongodb://172.18.160.16,172.18.160.17,172.18.160.18/db.offer?replicaSet=mongo-rs
    }

    def getFunctionFromURI(URI: String): String = {
        val functionName = URI match {
            case "http://users.ugent.be/~bjdmeest/function/grel.ttl#scale" => "scl"
            case "http://users.ugent.be/~bjdmeest/function/grel.ttl#substitute" => "substit"
            case "http://users.ugent.be/~bjdmeest/function/grel.ttl#skip" => "skp"
            case "http://users.ugent.be/~bjdmeest/function/grel.ttl#replace" => "replc"
            case "http://users.ugent.be/~bjdmeest/function/grel.ttl#prefix" => "prefix"
            case "http://users.ugent.be/~bjdmeest/function/grel.ttl#postfix" => "postfix"
            case "http://users.ugent.be/~bjdmeest/function/grel.ttl#toInt" => "toInt"
            case _ => ""
        }

        functionName
    }

}
