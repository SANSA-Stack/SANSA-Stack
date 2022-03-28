package net.sansa_stack.query.spark.rdd.op

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.aksw.jenax.arq.analytics.BindingAnalytics
import org.aksw.jenax.arq.util.binding.ResultSetUtils
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.exec.RowSetAdapter
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import scala.jdk.CollectionConverters.asScalaIteratorConverter

class PrefixAnalyticsTests extends FunSuite with DataFrameSuiteBase {

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
    conf
  }


  test("extracting prefixes from bindings should work") {

    val model = RDFDataMgr.loadModel("rdf.nt")
    val bindings: Seq[Binding] = new RowSetAdapter(
      QueryExecutionFactory.create("SELECT * { ?s ?p ?o }", model).execSelect()).asScala.toSeq

    val rdd: RDD[Binding] = sc.parallelize(bindings)
    import net.sansa_stack.query.spark._

    val evalResult = rdd.javaCollect(BindingAnalytics.usedPrefixes(6).asCollector())
    println(evalResult)

    val evalResult2 = rdd.javaCollect(BindingAnalytics.usedDatatypes.asCollector)
    println(evalResult2)


    // TODO Actually We want to test over all columns with a Map[Var, Set[String]]
    // val hack: RDD[String] = rdd.map(b => b.get(Vars.s)).filter(_.isURI).map(_.getURI)

    // FIXME The result does not look wrong but has not been manually verified
    // val expected: Set[String] = Set("http://commons.dbpedia.org/resource/Category:", "http://commons.dbpedia.org/resource/File:", "http://commons.dbpedia.org/resource/Template:Cc-by-")
    // val actual: Set[String] = Set.empty ++ RddOfAnyOps.aggregate(hack, () => new PrefixAccumulator(3)).asScala

    // assert(actual.equals(expected))
    // println(actual)
    // assert(successTriples.length == 1)
  }

}
