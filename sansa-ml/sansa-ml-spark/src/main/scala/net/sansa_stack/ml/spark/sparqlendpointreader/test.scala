package net.sansa_stack.ml.spark.sparqlendpointreader

object Test {

  def main(args: Array[String]): Unit = {
    val a = new RegressionEngieUsecaseTest()
      .setHost("http://localhost")
      .setPort("5001")
      .setHdfsHost("hdfs://localhost:8020/")
      .setSparql("sparql")
      .setSparqlQuery(
        "SELECT ?s ?p ?o WHERE { ?s ?r <http://engie/vocab/Building>. ?s ?p ?o. }"
      )
      .setOutputFileName("json2.json")
    a.run()

//    import scala.util.parsing.json._
//
//    val parsed =
//      JSON.parseFull("""{
//                                  |	"2m_temperature": {
//                                  |		"1498867200000": 301.2186584473,
//                                  |		"1498870800000": 300.5258483887,
//                                  |		"1498874400000": 298.7114868164,
//                                  |		"1498878000000": 298.3532409668,
//                                  |		"1498881600000": 298.3937072754,
//                                  |		"1498885200000": 301.0509338379,
//                                  |		"1498888800000": 301.2086791992
//                                  |	},
//                                  |	"clear_sky_direct_solar_radiation_at_surface": {
//                                  |		"1498867200000": 0.125,
//                                  |		"1498870800000": 0.125,
//                                  |		"1498874400000": 0.125,
//                                  |		"1498878000000": 0.125,
//                                  |		"1498881600000": 54255.625,
//                                  |		"1498885200000": 397691.125,
//                                  |		"1498888800000": 956592.3125
//                                  |	},
//                                  |	"snowfall": {
//                                  |		"1498867200000": 0.0,
//                                  |		"1498870800000": 0.0,
//                                  |		"1498874400000": 0.0,
//                                  |		"1498878000000": 0.0,
//                                  |		"1498881600000": 0.0,
//                                  |		"1498885200000": 0.0,
//                                  |		"1498888800000": 0.0
//                                  |	}
//                                  |}""".stripMargin)
//
//    println(parsed)
  }
}
