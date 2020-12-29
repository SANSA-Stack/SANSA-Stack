package net.sansa_stack.ml.spark.pipeline

import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.sys.JenaSystem


object SparqlifyInMl {
    def main(args: Array[String]): Unit = {

      // setup spark session
      val spark = SparkSession.builder
        .appName(s"Sparklify example")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", String.join(
          ", ",
          "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
          "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      JenaSystem.init()

      val inputFilePath = "/Users/carstendraschner/GitHub/SANSA-Stack/sansa-ml/sansa-ml-spark/src/main/resources/test.ttl"

      val queryString =
        """
          |SELECT ?seed ?seed__down_age ?seed__down_name ?seed__down_hasSpouse__down_age ?seed__down_hasParent__down_age ?seed__down_hasSpouse__down_name ?seed__down_hasParent__down_name
          |
          |WHERE {
          |	?seed a <http://dig.isi.edu/Person> .
          |
          |	OPTIONAL {
          |		?seed <http://dig.isi.edu/age> ?seed__down_age .
          |	}
          |	OPTIONAL {
          |		?seed <http://dig.isi.edu/name> ?seed__down_name .
          |	}
          |	OPTIONAL {
          |		?seed <http://dig.isi.edu/hasSpouse> ?seed__down_hasSpouse .
          |		?seed__down_hasSpouse <http://dig.isi.edu/age> ?seed__down_hasSpouse__down_age .
          |	}
          |	OPTIONAL {
          |		?seed <http://dig.isi.edu/hasParent> ?seed__down_hasParent .
          |		?seed__down_hasParent <http://dig.isi.edu/age> ?seed__down_hasParent__down_age .
          |	}
          |	OPTIONAL {
          |		?seed <http://dig.isi.edu/hasSpouse> ?seed__down_hasSpouse .
          |		?seed__down_hasSpouse <http://dig.isi.edu/name> ?seed__down_hasSpouse__down_name .
          |	}
          |	OPTIONAL {
          |		?seed <http://dig.isi.edu/hasParent> ?seed__down_hasParent .
          |		?seed__down_hasParent <http://dig.isi.edu/name> ?seed__down_hasParent__down_name .
          |	}
          |}""".stripMargin

      val triples = spark.rdf(Lang.TURTLE)(inputFilePath).cache()

      // triples.foreach(println(_))

      import net.sansa_stack.query.spark.query._

      val resDf = triples.sparql(queryString)
      resDf.show(false)

    }
}
