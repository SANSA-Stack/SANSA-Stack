package net.sansa_stack.datalake.spark

import java.util

import org.apache.jena.query.{QueryExecutionFactory, QueryFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.util.FileManager

import scala.collection.mutable
//import org.mongodb.scala._

/**
  * Created by mmami on 26.07.17.
  */
class Helpers() {

}

object Helpers {
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

    def omitQuestionMark(str: String): String = str.replace("?","")


    def omitNamespace(URI: String): String = {
        val URIBits = URI.replace("<","").replace(">","").replace("#","/").split("/")
        URIBits(URIBits.length-1)
    }

    def getNamespaceFromURI(URI: String): String = {
        "" // TODO: create
    }

    def get_NS_predicate(predicateURI: String): (String, String) = {

        val url = predicateURI.replace("<","").replace(">","")
        val URIBits = url.split("/")

        var pred = ""
        if(predicateURI.contains("#")) {
            pred = URIBits(URIBits.length-1).split("#")(1) // like: http://www.w3.org/2000/01/[rdf-schema#label]
        } else {
            pred = URIBits(URIBits.length-1)
        }

        val ns = url.replace(pred, "")

        (ns,pred)
    }

    def getTypeFromURI(typeURI: String) : String = {
        var dataType = typeURI.split("#") // from nosql ns

        var rtrn = dataType(dataType.length-1)

        rtrn
    }

    def getSelectColumnsFromSet(pred_attr: mutable.HashMap[String,String],
                                star: String,
                                prefixes: Map[String, String],
                                select: util.List[String],
                                star_predicate_var: mutable.HashMap[(String, String), String],
                                neededPredicates: mutable.Set[String]
        ): String = {

        var columns = ""
        var i = 0

        for (v <- pred_attr) {
            //println("pred_attr: " + pred_attr)
            val attr = v._2
            val ns_predicate = Helpers.get_NS_predicate(v._1)

            val ns_predicate_bits = ns_predicate
            val NS = ns_predicate_bits._1
            val predicate = ns_predicate_bits._2

            val objVar = star_predicate_var(("?" + star, "<" + NS + predicate + ">"))

            println("-> Variable: " + objVar + " exists in WHERE, is it in SELECT? " + select.contains(objVar.replace("?","")))

            //if (select.contains(objVar.replace("?",""))) {
            if (neededPredicates.contains(v._1)) {
                val c = attr + " AS `" + star + "_" + predicate + "_" + prefixes(NS) + "`"
                //println("SELECT CLAUSE: " + c)
                //columns = if(i == 0) columns + v else columns + "," + columns
                if (i == 0) columns += c else columns += "," + c
                i += 1
            }
        }

        columns
    }

    def getID(sourcePath: String, mappingsFile: String): String = {
        //var mappingsFile = Config.get("mappings.file")

        var getID = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#>" +
            "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
            "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
            "SELECT ?t WHERE {" +
                "?mp rml:logicalSource ?ls . " +
                "?ls rml:source \"" + sourcePath + "\" . " +
                "?mp rr:subjectMap ?sm . " +
                "?sm rr:template ?t " +
            "}"

        //println("GOING TO EXECUTE: " + getID)

        var in = FileManager.get().open(mappingsFile)

        var model = ModelFactory.createDefaultModel()
        model.read(in, null, "TURTLE")

        var id = ""

        var query1 = QueryFactory.create(getID)
        var qe1 = QueryExecutionFactory.create(query1, model)
        var results1 = qe1.execSelect()
        while (results1.hasNext) {
            var soln1 = results1.nextSolution()
            var template = soln1.get("t").toString

            var templateBits = template.split("/")
            id = templateBits(templateBits.length-1).replace("{","").replace("}","")
        }

        id
    }

    def makeMongoURI(uri: String, database: String, collection: String, options: String) = {
        if(options == null)
            s"mongodb://${uri}/${database}.${collection}"
        else
            s"mongodb://${uri}/${database}.${collection}?${options}"
        //mongodb://db1.example.net,db2.example.net:27002,db3.example.net:27003/?db_name&replicaSet=YourReplicaSetName
        //mongodb://172.18.160.16,172.18.160.17,172.18.160.18/db.offer?replicaSet=mongo-rs
    }

    // Special MongoDB helpers
    /*implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
        override val converter: (Document) => String = (doc) => doc.toJson
    }

    implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
        override val converter: (C) => String = (doc) => doc.toString
    }

    trait ImplicitObservable[C] {
        val observable: Observable[C]
        val converter: (C) => String

        def results(): Seq[C] = Await.result(observable.toFuture(), Duration(100, TimeUnit.SECONDS))
        def headResult() = Await.result(observable.head(), Duration(100, TimeUnit.SECONDS))
        def printResults(initial: String = ""): Unit = {
            if (initial.length > 0) print(initial)
            results().foreach(res => println(converter(res)))
        }
        def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
    }*/

}
