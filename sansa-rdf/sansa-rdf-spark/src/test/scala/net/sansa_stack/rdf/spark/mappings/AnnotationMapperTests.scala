package net.sansa_stack.rdf.spark.mappings

import java.util.Objects

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.mapper.{Cluster, ClusterEntry, LevenshteinMatcherConfig, Person}
import org.aksw.jenax.reprogen.core.JenaPluginUtils
import org.apache.jena.rdf.model.{ModelFactory, Property, Resource, ResourceFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import spire.std.LevenshteinDistance

class AnnotationMapperTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.io._
  import net.sansa_stack.rdf.spark.ops._

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
    conf
  }

  test("extracting first names of people from natural trig resources should work") {
    JenaPluginUtils.scan(classOf[Person])

    val path = getClass.getClassLoader.getResource("people.trig").getPath

    val names = spark
      .datasets(Lang.TRIG)(path)
      .mapToNaturalResources()
      .mapAs(classOf[Person])
      .map(_.getFirstName)
      .collect()

    assert(names(0) == "Ana")
    assert(names(1) == "Bob")
    assert(names(2) == "Bna")
  }


  def createLevenshteinMatcher(nonSerializeConfig: LevenshteinMatcherConfig): RDD[Resource] => RDD[Resource] = {
    // Somehow spark is not smart enough to serialize the following from the lambda
    // serializableConfig = nonSerializeConfig.asResource()

    // The broadcast is a workaround
    // The .asResource() is crucial here: It unproxies the config object (derived from Resource) into a plain ResourceImpl
    val conf = spark.sparkContext.broadcast(nonSerializeConfig.asResource())

    in => {
      // Ensure to cache the data before the cartesion
      in.repartition(100).cache().take(1)

      val outRdd: RDD[Resource] = in
        .cartesian(in)
        .groupBy(_._1)
        .map(x => {
          val config = conf.value.as(classOf[LevenshteinMatcherConfig])
          val pStr = config.getOnProperty

          val p: Property = ResourceFactory.createProperty(pStr)

          val m = ModelFactory.createDefaultModel()

          val l = x._1
          val lhs = Objects.toString(l.getProperty(p).getLiteral.getValue)

          val cluster = m.createResource().as(classOf[Cluster])
            .setClusterTarget(l)
            .setConfig(config)

          m.add(config.getModel)
          m.add(l.getModel)

          for (e <- x._2) {
            val r = e._2
            m.add(r.getModel)

            val rhs = Objects.toString(r.getProperty(p).getLiteral.getValue)
            val d = LevenshteinDistance.distance(lhs, rhs)

            val clusterEntry = m.createResource().as(classOf[ClusterEntry])
              .setItem(r)
              .setValue(d)

            cluster.getMembers.add(clusterEntry)
          }
          cluster.asResource()
        })

      outRdd
    }
  }

  /**
   * TODO Validate output
   *
   * Test for working with resource views on the example of clustering resources
   * using levenshtein distance. Note that the implementation just uses a naive cartesian product
   * and distributes resources and their RDF graphs 'as-they-are'.
   * What we would actually need is a catalyst optimization for RDF resources:
   * whereas catalyst can figure out that e.g. dataset.getColumn('foo') may be exclusive access to
   * column foo (and thus all othere columns can be discarded), there is no such magic for
   * resource.getProperty(NS.foo).
   *
   *
   */
  test("creating a pure RDF model for clustering should work") {

    JenaPluginUtils.scan(classOf[LevenshteinMatcherConfig])

    val path = getClass.getClassLoader.getResource("people.trig").getPath

    val config = ModelFactory.createDefaultModel().createResource("http://www.example.org/myConfig")
      .as(classOf[LevenshteinMatcherConfig])
      .setOnProperty(FOAF.firstName.getURI)
      .setThreshold(10)


    val transformer: RDD[Resource] => RDD[Resource] = createLevenshteinMatcher(config)

    val input = spark
      .datasets(Lang.TRIG)(path)
      .mapToNaturalResources()

    val output = transformer.apply(input)
      .collect
      .map(_.as(classOf[Cluster]))

    for(cluster <- output) {
      println("Cluster for " + cluster.getClusterTarget.asResource().getURI + ": ")
      RDFDataMgr.write(System.out, cluster.getModel, RDFFormat.TURTLE_PRETTY)
    }

    /* The output for a single cluster now may look like
    Cluster for http://www.example.org/Bna:

[ <http://www.example.org/clusterTarget>
          <http://www.example.org/Bna> ;
  <http://www.example.org/config>
          <http://www.example.org/myConfig> ;
  <http://www.example.org/members>
          [ <http://www.example.org/item>   <http://www.example.org/Bna> ;
            <http://www.example.org/value>  "0"^^<http://www.w3.org/2001/XMLSchema#int>
          ] ;
  <http://www.example.org/members>
          [ <http://www.example.org/item>   <http://www.example.org/Ana> ;
            <http://www.example.org/value>  "1"^^<http://www.w3.org/2001/XMLSchema#int>
          ] ;
  <http://www.example.org/members>
          [ <http://www.example.org/item>   <http://www.example.org/Bob> ;
            <http://www.example.org/value>  "2"^^<http://www.w3.org/2001/XMLSchema#int>
          ]
] .

<http://www.example.org/myConfig>
        a       <http://www.example.org/LevenshteinMatcherConfig> ;
        <http://www.example.org/onProperty>
                <http://xmlns.com/foaf/0.1/firstName> ;
        <http://www.example.org/threshold>
                "10"^^<http://www.w3.org/2001/XMLSchema#int> .

<http://www.example.org/Ana>
        <http://xmlns.com/foaf/0.1/firstName>
                "Ana" ;
        <http://xmlns.com/foaf/0.1/givenName>
                "Nan" .


<http://www.example.org/Bob>
        <http://xmlns.com/foaf/0.1/firstName>
                "Bob" ;
        <http://xmlns.com/foaf/0.1/givenName>
                "Obo" .

<http://www.example.org/Bna>
        <http://xmlns.com/foaf/0.1/firstName>
                "Bna" ;
        <http://xmlns.com/foaf/0.1/givenName>
                "Anb" .
     */


    // Uncomment to read config from RDF file
    /*
    val config = RDFDataMgr.loadModel("rdf-mapper/config.ttl")
      .listSubjectsWithProperty(RDF.`type`, null)
      .nextOptional()
      .map(r => r.as(classOf[LevenshteinMatcherConfig]))
      .orElseThrow(() => new RuntimeException("Expected a config"))
    */
  }

}
