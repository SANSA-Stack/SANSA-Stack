package net.sansa_stack.inference.spark.forwardchaining.triples

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.data.model.{RDFGraph, RDFGraphDataFrame}
import net.sansa_stack.inference.spark.utils.RDFSSchemaExtractor
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

import scala.language.implicitConversions



/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @constructor create a new RDFS forward chaining reasoner
  * @param session the Apache Spark session
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerRDFSDataframe(session: SparkSession, parallelism: Int = 2)
  extends TransitiveReasoner(session.sparkContext, parallelism) {

  val sqlContext = session.sqlContext
  import sqlContext.implicits._

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  def apply(graph: RDFGraphDataFrame): RDFGraphDataFrame = {
    logger.info("materializing graph...")
    val startTime = System.currentTimeMillis()

    val sqlSchema = graph.schema

    val extractor = new RDFSSchemaExtractor()

    var index = extractor.extractWithIndex(graph)

    var triples = graph.toDataFrame(session).alias("DATA")

    // broadcast the tables for the schema triples
    index = index.map{ e =>
      val property = e._1
      val dataframe = e._2

      property -> broadcast(dataframe).as(property.getURI)
    }

    // RDFS rules dependency was analyzed in \todo(add references) and the same ordering is used here


    // 1. we first compute the transitive closure of rdfs:subPropertyOf and rdfs:subClassOf

    /**
      * rdfs11 xxx rdfs:subClassOf yyy .
      * yyy rdfs:subClassOf zzz . xxx rdfs:subClassOf zzz .
     */
    val subClassOfTriples = index(RDFS.subClassOf.asNode()) // extract rdfs:subClassOf triples
    val subClassOfTriplesTrans = broadcast(computeTransitiveClosureDF(subClassOfTriples.as[RDFTriple]).toDF().alias("SC"))
//    val subClassOfMap = CollectionUtils.toMultiMap(subClassOfTriplesTrans.rdd.map(r => (r.getString(0) -> (r.getString(2)))).collect)
//    val subClassOfMapBC = session.sparkContext.broadcast(subClassOfMap)
//    val checkSubclass = udf((cls: String) => subClassOfMapBC.value.contains(cls))
//    val makeSuperTypeTriple = udf((ind: String, cls: String) => (ind, subClassOfMapBC.value(cls)))
    /*
        rdfs5	xxx rdfs:subPropertyOf yyy .
              yyy rdfs:subPropertyOf zzz .	xxx rdfs:subPropertyOf zzz .
     */
    val subPropertyOfTriples = index(RDFS.subPropertyOf.asNode()) // extract rdfs:subPropertyOf triples
    val subPropertyOfTriplesTrans = broadcast(computeTransitiveClosureDF(subPropertyOfTriples.as[RDFTriple]).toDF().alias("SP"))


//    // a map structure should be more efficient
//    val subClassOfMap = subClassOfTriplesTrans.collect().map(row => row(0).asInstanceOf[String] -> row(1).asInstanceOf[String]).toMap
//    val subPropertyOfMap = subPropertyOfTriplesTrans.collect().map(row => row(0).asInstanceOf[String] -> row(1).asInstanceOf[String]).toMap
//
//    // distribute the schema data structures by means of shared variables
//    // the assumption here is that the schema is usually much smaller than the instance data
//    val subClassOfMapBC = session.sparkContext.broadcast(subClassOfMap)
//    val subPropertyOfMapBC = session.sparkContext.broadcast(subPropertyOfMap)
//
//    def containsPredicateAsKey(map: Map[String, String]) = udf((predicate : String) => map.contains(predicate))
//    def fillPredicate(map: Map[String, String]) = udf((predicate : String) => if(map.contains(predicate)) map(predicate) else "")


    // Broadcast
//    val subClassOfTriplesTransDataBC = session.sparkContext.broadcast(subPropertyOfTriplesTrans.collectAsList())
//    val subClassOfTriplesTransSchemaBC = session.sparkContext.broadcast(subPropertyOfTriplesTrans.schema)
//    val subClassOfTriplesTransBCDF = session.sqlContext.createDataFrame(
//                                        subClassOfTriplesTransDataBC.value,
//                                        subClassOfTriplesTransSchemaBC.value).alias("SCBC")

    // 2. SubPropertyOf inheritance according to rdfs7 is computed

    /*
      rdfs7	aaa rdfs:subPropertyOf bbb .
            xxx aaa yyy .                   	xxx bbb yyy .
     */
    val triplesRDFS7 =
      triples // all triples (s p1 o)
      .join(subPropertyOfTriplesTrans, $"DATA.${sqlSchema.predicateCol}" === $"SP.${sqlSchema.subjectCol}", "inner") // such that p1 has a super property p2
      .select($"DATA.${sqlSchema.subjectCol}", $"SP.${sqlSchema.objectCol}", $"DATA.${sqlSchema.objectCol}") // create triple (s p2 o)

//    val triplesRDFS7 =
//      triples // all triples (s p1 o)
//        .filter(containsPredicateAsKey(subPropertyOfMapBC.value)($"DATA.predicate")) // such that p1 has a super property p2
//        .withColumn("CC", fillPredicate(subPropertyOfMapBC.value)($"DATA.predicate"))
//        .select($"DATA.subject", $"CC", $"DATA.object") // create triple (s p2 o)
//
//    triplesRDFS7.explain(true)

    // add triples
    triples = triples.union(triplesRDFS7).alias("DATA")

    // 3. Domain and Range inheritance according to rdfs2 and rdfs3 is computed

    /*
    rdfs2	aaa rdfs:domain xxx .
          yyy aaa zzz .	          yyy rdf:type xxx .
     */
    val domainTriples = broadcast(index(RDFS.domain.asNode()).alias("DOM"))

    val triplesRDFS2 =
      triples
        .join(domainTriples, $"DATA.${sqlSchema.predicateCol}" === $"DOM.${sqlSchema.subjectCol}", "inner")
        .select($"DATA.${sqlSchema.subjectCol}", $"DOM.${sqlSchema.objectCol}") // (yyy, xxx)
//    triples.createOrReplaceTempView("DATA")
//    domainTriples.createOrReplaceTempView("DOM")
//    val triplesRDFS2 = session.sql("SELECT A.subject, B.object FROM DATA A INNER JOIN DOM B ON A.predicate=B.subject")
//    triplesRDFS2.explain(true)

    /*
   rdfs3	aaa rdfs:range xxx .
         yyy aaa zzz .	          zzz rdf:type xxx .
    */
    val rangeTriples = broadcast(index(RDFS.range.asNode()).alias("RAN"))

    val triplesRDFS3 =
      triples
        .join(rangeTriples, $"DATA.${sqlSchema.predicateCol}" === $"RAN.${sqlSchema.subjectCol}", "inner")
        .select($"DATA.${sqlSchema.objectCol}", $"RAN.${sqlSchema.objectCol}") // (zzz, xxx)

    val tuples23 = triplesRDFS2.union(triplesRDFS3)

    // get rdf:type tuples here as intermediate result
    val typeTuples = triples
      .where(s"${sqlSchema.predicateCol} = '${RDF.`type`.getURI}'")
      .select(sqlSchema.subjectCol, sqlSchema.objectCol)
      .union(tuples23)
      .alias("TYPES")

    // 4. SubClass inheritance according to rdfs9

    /*
    rdfs9	xxx rdfs:subClassOf yyy .
          zzz rdf:type xxx .	        zzz rdf:type yyy .
     */
    val tuplesRDFS9 = typeTuples
      .join(subClassOfTriplesTrans, $"TYPES.${sqlSchema.objectCol}" === $"SC.${sqlSchema.subjectCol}", "inner")
      .select($"TYPES.${sqlSchema.subjectCol}", $"SC.${sqlSchema.objectCol}") // (zzz, yyy)

//    val triplesRDFS9 =
//      typeTuples
//          .where(checkSubclass($"TYPES.object"))
//          .map(r => (r.getString(0), subClassOfMapBC.value(r.getString(1)).toArray))
//      .toDF("subject", "objects")
//    triplesRDFS9.printSchema()
//
//        val exploded = triplesRDFS9.flatMap(row => {
//          val objects = row.getAs[Array[String]]("objects")
//          objects.map(o => (row.getString(0), o))
//        }).toDF("subject", "object")

//          explode("objects", "object") {
//          case Row(classes: Array[Row]) => classes.map(clsRow => clsRow(0).asInstanceOf[String])
//          case _ => println("ELSE")
//            Seq()
//        }
//    exploded.show()

//      .explode()
//        .join(subClassOfTriplesTrans, $"TYPES.object" === $"SC.subject", "inner")
//        .withColumn("const", lit(RDF.`type`.getURI))
//        .select("DATA.subject", "const", "SC.object")
//        .select($"TYPES.subject", $"SC.object") // (zzz, yyy)
//    println("existing types:" + existingTypes.count())
//    println("SC:" + subClassOfTriplesTrans.count())
//    println("SP:" + subPropertyOfTriplesTrans.count())
//    println("TYPES:" + typeTuples.count())
//    println("R7:" + triplesRDFS7.count())
//    println("R2:" + triplesRDFS2.count())
//    println("R3:" + triplesRDFS3.count())
//    println("R9:" + tuplesRDFS9.count())

    // 5. merge triples and remove duplicates
    val allTriples =
      typeTuples.union(tuples23).union(tuplesRDFS9)
        .withColumn("const", lit(RDF.`type`.getURI))
        .select(sqlSchema.subjectCol, "const", sqlSchema.objectCol)
      .union(subClassOfTriplesTrans)
      .union(subPropertyOfTriplesTrans)
      .union(triplesRDFS7)
      .union(triples)
      .distinct()
//        .selectExpr("subject", "'" + RDF.`type`.getURI + "' as predicate", "object")
//    allTriples.explain()

    logger.info("...finished materialization in " + (System.currentTimeMillis() - startTime) + "ms.")
//    val newSize = allTriples.count()
//    logger.info(s"|G_inf|=$newSize")

    // return graph with inferred triples
    new RDFGraphDataFrame(allTriples)
  }

  /**
    * Computes the transitive closure for a Dataframe of triples
    *
    * @param edges the Dataframe of triples
    * @return a Dataframe containing the transitive closure of the triples
    */
  def computeTransitiveClosureDF(edges: Dataset[RDFTriple]): Dataset[RDFTriple] = {
    log.info("computing TC...")
    //    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[RDFTriple]
    val spark = edges.sparkSession.sqlContext
    import spark.implicits._

//    profile {
      // we keep the transitive closure cached
      var tc = edges
      tc.cache()

      // the join is iterated until a fixed point is reached
      var i = 1
      var oldCount = 0L
      var nextCount = tc.count()
      do {
        log.info(s"iteration $i...")
        oldCount = nextCount

        //        val df1 = tc.alias("df1")
        //        val df2 = tc.alias("df2")
        // perform the join (x, y) x (y, x), obtaining an RDD of (x=y, (y, x)) pairs,
        // then project the result to obtain the new (x, y) paths.

        tc.createOrReplaceTempView("SC")
        var joined = tc.as("A").join(tc.as("B"), $"A.o" === $"B.s").select("A.s", "A.p", "B.o").as[RDFTriple]
        //          var joined = tc
        //            .join(edges, tc("o") === edges("s"))
        //            .select(tc("s"), tc("p"), edges("o"))
        //            .as[RDFTriple]
        //        tc.sqlContext.
        //          sql("SELECT A.subject, A.predicate, B.object FROM SC A INNER JOIN SC B ON A.object = B.subject")

        //      joined.explain()
        //      var joined = df1.join(df2, df1("object") === df2("subject"), "inner")
        //      println("JOINED:\n" + joined.collect().mkString("\n"))
        //      joined = joined.select(df2(s"df1.$col1"), df1(s"df1.$col2"))
        //      println(joined.collect().mkString("\n"))

        tc = tc
          .union(joined)
          .distinct()
          .cache()
        nextCount = tc.count()
        i += 1
      } while (nextCount != oldCount)

      tc.sqlContext.uncacheTable("SC")
      log.info("TC has " + nextCount + " edges.")
      tc
//    }
  }

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  override def apply(graph: RDFGraph): RDFGraph = graph

}

object ForwardRuleReasonerRDFSDataframe {
  def apply(session: SparkSession, parallelism: Int = 2): ForwardRuleReasonerRDFSDataframe = new ForwardRuleReasonerRDFSDataframe(session, parallelism)

  def main(args: Array[String]): Unit = {
    import net.sansa_stack.inference.spark.data.loader.sql.rdf._

    val parallelism = 2

    // register the custom classes for Kryo serializer
    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[org.apache.jena.graph.Triple]))
    conf.set("spark.extraListeners", "net.sansa_stack.inference.spark.utils.CustomSparkListener")

    // the SPARK config
    val session = SparkSession.builder
      .appName(s"SPARK DataFrame-based RDFS Reasoning")
      .master("local[4]")
//      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.shuffle.partitions", parallelism)
      .config(conf)
      .getOrCreate()

    val triples = session.read.rdf(Lang.NTRIPLES)(args(0))
    triples.createOrReplaceTempView("TRIPLES")

    val graph = new RDFGraphDataFrame(triples)


    val infGraph = ForwardRuleReasonerRDFSDataframe(session).apply(graph)
    println(infGraph.size())
  }
}