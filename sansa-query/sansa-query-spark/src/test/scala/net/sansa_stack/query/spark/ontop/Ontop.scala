package net.sansa_stack.query.spark.ontop

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.query.{QueryFactory, ResultSetFormatter}
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.resultset.ResultsFormat
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.SparkSession
/**
 * @author Lorenz Buehmann
 */
object Ontop {
  def main(args: Array[String]): Unit = {
    JenaSystem.init()
    QueryFactory.create(args(1))
    QueryFactory.create(args(2))

    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrationRequired", "true")
      // .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      .config("spark.sql.cbo.enabled", true)
      .config("spark.sql.statistics.histogram.enabled", true)
      .config("spark.sql.crossJoin.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()

    runWithTwoDatasetsLoaded(spark, "/home/user/work/datasets/soccerplayer.nt", "/home/user/work/datasets/soccerplayer_100.nt")

//    val triples = spark.rdf(Lang.NTRIPLES)(args(0)).cache()
//
//    val queryEngineFactory = new QueryEngineFactoryOntop(spark)
//
//    val qef1 = queryEngineFactory.create(triples)
//    val qe1 = qef1.createQueryExecution(args(1))
//    val res1 = qe1.execConstructSpark
//
//    val qef2 = queryEngineFactory.create(res1)
//    val qe2 = qef2.createQueryExecution(args(2))
//    val res2 = qe2.execSelect()
//
//    ResultSetFormatter.output(res2, ResultsFormat.FMT_TEXT)

    spark.close()
  }

  def runWithTwoDatasetsLoaded(spark: SparkSession, path1: String, path2: String): Unit = {
    val triples1 = spark.rdf(Lang.NTRIPLES)(path1).cache()

    val triples2 = spark.rdf(Lang.NTRIPLES)(path2).cache()

    val queryEngineFactory = new QueryEngineFactoryOntop(spark)

    val query = "select * {?s <http://dbpedia.org/ontology/team> ?o}"

    val qef1 = queryEngineFactory.create(triples1)
    val qe1 = qef1.createQueryExecution(query)
    val res1 = qe1.execSelect()
    ResultSetFormatter.output(res1, ResultsFormat.FMT_TEXT)

    val qef2 = queryEngineFactory.create(triples2)
    val qe2 = qef2.createQueryExecution(query)
    val res2 = qe2.execSelect()
    ResultSetFormatter.output(res2, ResultsFormat.FMT_TEXT)

    val qe12 = qef1.createQueryExecution(query)
    val res12 = qe12.execSelect()
    ResultSetFormatter.output(res12, ResultsFormat.FMT_TEXT)


  }
}
