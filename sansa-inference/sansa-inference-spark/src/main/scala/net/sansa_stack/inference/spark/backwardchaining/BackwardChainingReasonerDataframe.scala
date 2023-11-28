package net.sansa_stack.inference.spark.backwardchaining


import net.sansa_stack.inference.rules.RuleSets
import net.sansa_stack.inference.rules.plan.SimpleSQLGenerator
import net.sansa_stack.inference.spark.backwardchaining.tree.{AndNode, OrNode}
import net.sansa_stack.inference.utils.RuleUtils._
import net.sansa_stack.inference.utils.{CollectionUtils, Logging, TripleUtils}
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.rdf.model.Resource
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.reasoner.rulesys.impl.BindingVector
import org.apache.jena.sparql.util.FmtUtils
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader


// case class RDFTriple(s: Node, p: Node, o: Node)
case class RDFTriple(s: String, p: String, o: String)

/**
  * @author Lorenz Buehmann
  */
class BackwardChainingReasonerDataframe(
                                         val session: SparkSession,
                                         val rules: Set[Rule],
                                         val graph: Dataset[RDFTriple]) extends Logging {

  import org.apache.spark.sql.functions._
  private implicit def resourceToNodeConverter(resource: Resource): Node = resource.asNode()

  val precomputeSchema: Boolean = true

  lazy val schema: Map[Node, Dataset[RDFTriple]] = if (precomputeSchema) extractWithIndex(graph) else Map()

  def isEntailed(triple: Triple): Boolean = {
    isEntailed(new TriplePattern(triple))
  }

  def isEntailed(tp: TriplePattern): Boolean = {
    val tree = buildTree(new AndNode(tp), Seq())
    log.info(tree.toString())

    val triples = processTree(tree)
    triples.explain(true)
    log.info(triples.distinct().count().toString())

    false
  }



  private def processTree(tree: AndNode): Dataset[RDFTriple] = {
    // 1. look for asserted triples in the graph
    val assertedTriples = lookup(tree.element)
    if(TripleUtils.isTerminological(tree.element.asTriple())) broadcast(assertedTriples)


    // 2. process the inference rules that can infer the triple pattern
    val inferredTriples = tree.children.map(child => {
      log.info(s"processing rule ${child.element}")

      // first process the children, i.e. we get the data for each triple pattern in the body of the rule
      val childrenTriples: Seq[Dataset[RDFTriple]] = child.children.map(processTree(_))

      val baseTriples = if (childrenTriples.size > 1) childrenTriples.reduce(_ union _) else childrenTriples.head


      // then apply the rule on the UNION of the children data
      applyRule(child.element, baseTriples)
    })

    var triples = assertedTriples

    if(inferredTriples.nonEmpty) triples = triples.union(inferredTriples.reduce(_ union _))

    triples
  }

  private def lookup(tp: TriplePattern): Dataset[RDFTriple] = {
    lookup(tp.asTriple())
  }

  private def lookupSimple(tp: Triple, triples: Dataset[RDFTriple] = graph): Dataset[RDFTriple] = {
    info(s"Lookup data for $tp")
    val s = tp.getSubject.toString()
    val p = tp.getPredicate.toString()
    val o = tp.getObject.toString()

    var filteredGraph = triples

    if(tp.getSubject.isConcrete) {
      filteredGraph.filter(t => t.s.equals(s))
    }
    if(tp.getPredicate.isConcrete) {
      filteredGraph = filteredGraph.filter(t => t.p.equals(p))
    }
    if(tp.getObject.isConcrete) {
      filteredGraph = filteredGraph.filter(t => t.o.equals(o))
    }
    filteredGraph
  }

  private def lookup(tp: Triple): Dataset[RDFTriple] = {

    val terminological = TripleUtils.isTerminological(tp)

    var filteredGraph =
      if (terminological) {
        schema.getOrElse(tp.getPredicate, graph)
      } else {
        graph
      }

    info(s"Lookup data for $tp")
    val s = tp.getSubject.toString()
    val p = tp.getPredicate.toString()
    val o = tp.getObject.toString()

    if(tp.getSubject.isConcrete) {
      filteredGraph = filteredGraph.filter(t => t.s.equals(s))
    }
    if(!terminological && tp.getPredicate.isConcrete) {
      filteredGraph = filteredGraph.filter(t => t.p.equals(p))
    }
    if(tp.getObject.isConcrete) {
      filteredGraph = filteredGraph.filter(t => t.o.equals(o))
    }
    filteredGraph
  }

  private def buildTree(tree: AndNode, visited: Seq[Rule]): AndNode = {
    val tp = tree.element

    rules.filterNot(visited.contains(_)).foreach(r => {
      // check if the head is more general than the triple in question
      val head = r.headTriplePatterns()

      head.foreach(headTP => {
        val subsumes = headTP.subsumes(tp)

        if(subsumes) {
          // instantiate the rule
          val boundRule = instantiateRule(r, tp)

          // add new Or-node to tree
          val node = new OrNode(boundRule)
          //          println(node)
          tree.children :+= node

          boundRule.bodyTriplePatterns().foreach(newTp => {
            node.children :+= buildTree(new AndNode(newTp), visited ++ Seq(r))
          })
        }
      })

    })

    tree
  }

  /*
  // create a binding for the rule variables
   */
  private def instantiateRule(rule: Rule, tp: TriplePattern): Rule = {
    val headTP = rule.headTriplePatterns().head // TODO handle rules with multiple head TPs

    val binding = new BindingVector(5)

    // the subject
    if(tp.getSubject.isConcrete && headTP.getSubject.isVariable) {
      binding.bind(headTP.getSubject, tp.getSubject)
    }
    // the predicate
    if(tp.getPredicate.isConcrete && headTP.getPredicate.isVariable) {
      binding.bind(headTP.getPredicate, tp.getPredicate)
    }
    // the object
    if(tp.getObject.isConcrete && headTP.getObject.isVariable) {
      binding.bind(headTP.getObject, tp.getObject)
    }

    rule.instantiate(binding)
  }

  import session.implicits._

  private def applyRule(rule: Rule, dataset: Dataset[RDFTriple]): Dataset[RDFTriple] = {
    // convert to SQL
    val sqlGenerator = new SimpleSQLGenerator()
    var sql = sqlGenerator.generateSQLQuery(rule)
//    val sql =
//      """
//        |SELECT rel0.s, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' AS p, 'http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person' AS o
//        | FROM TRIPLES rel1 INNER JOIN TRIPLES rel0 ON rel1.s=rel0.p
//        | WHERE rel1.o='http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person' AND rel1.p='http://www.w3.org/2000/01/rdf-schema#domain'
//      """.stripMargin

    // generate logical execution plan
//    val planGenerator = new SimplePlanGenerator(TriplesSchema.get())
//    val plan = planGenerator.generateLogicalPlan(rule)

    val tableName = s"TRIPLES_${rule.getName}"
    sql = sql.replace("TRIPLES", tableName)
    log.info(s"SQL NEW: $sql")
    dataset.createOrReplaceTempView(tableName)
    dataset.sparkSession.sql(sql).as[RDFTriple]
  }

  val properties = Set(
    (RDFS.subClassOf, true, "SCO"),
    (RDFS.subPropertyOf, true, "SPO"),
    (RDFS.domain, false, "DOM"),
    (RDFS.range, false, "RAN"))



  val DUMMY_VAR = NodeFactory.createVariable("VAR")

  /**
    * Computes the triples for each schema property p, e.g. `rdfs:subClassOf` and returns it as mapping from p
    * to the [[Dataset]] containing the triples.
    *
    * @param graph the RDF graph
    * @return a mapping from the corresponding schema property to the Dataframe of s-o pairs
    */
  def extractWithIndex(graph: Dataset[RDFTriple]): Map[Node, Dataset[RDFTriple]] = {
    log.info("Started schema extraction...")

    val bcProperties = session.sparkContext.broadcast(Set(
      RDFS.subClassOf,
      RDFS.subPropertyOf,
      RDFS.domain,
      RDFS.range).map(_.toString()))


    val schemaTriples = graph.filter(t => bcProperties.value.contains(t.p)).cache()

    // for each schema property p
    val index =
      properties.map { entry =>
        val p = entry._1
        val tc = entry._2
        val alias = entry._3

        // get triples (s, p, o)
        var triples = lookupSimple(Triple.create(DUMMY_VAR, p, DUMMY_VAR), schemaTriples)

        // compute TC if necessary
        if (tc) triples = computeTC(triples)

        // broadcast the triples
        triples = broadcast(triples).alias(alias)

        // register as a table
        triples.createOrReplaceTempView(FmtUtils.stringForNode(p).replace(":", "_"))

        // add to index
        (p.asNode() -> triples)
      }
    log.info("Finished schema extraction.")

    index.toMap
  }

  def query(tp: Triple): Dataset[RDFTriple] = {
    import org.apache.spark.sql.functions._

    val domain = schema.getOrElse(RDFS.domain, broadcast(graph.filter(t => t.p == RDFS.domain.toString())).alias("DOMAIN"))
    val range = schema.getOrElse(RDFS.range, broadcast(graph.filter(t => t.p == RDFS.range.toString())).alias("RANGE"))
    val sco = schema.getOrElse(RDFS.subClassOf, broadcast(computeTC(graph.filter(t => t.p == RDFS.subClassOf.toString()))).alias("SCO"))
    val spo = schema.getOrElse(RDFS.subPropertyOf, broadcast(computeTC(graph.filter(t => t.p == RDFS.subPropertyOf.toString()))).alias("SPO"))

    // asserted triples
    var ds = lookup(tp)

    // inferred triples
    if(tp.getPredicate.isConcrete) {

      if (tp.getPredicate.matches(RDF.`type`.asNode())) { // rdf:type data

        var instanceTriples = graph

        // if s is concrete, we filter first
        if(tp.getSubject.isConcrete) { // find triples where s occurs as subject or object
          instanceTriples = instanceTriples.filter(t => t.s == tp.getSubject.toString() || t.o == tp.getSubject.toString())
        }

        // get all non rdf:type triples
        instanceTriples = instanceTriples.filter(_.p != RDF.`type`.toString())

        // enrich the instance data with super properties, i.e. rdfs5
        if(tp.getSubject.isConcrete) { // find triples where s occurs as subject or object
          instanceTriples = instanceTriples.filter(t => t.s == tp.getSubject.toString() || t.o == tp.getSubject.toString())
        }
        val spoBC = session.sparkContext.broadcast(
          CollectionUtils.toMultiMap(spo.select("s", "o").collect().map(r => (r.getString(0), r.getString(1))))
        )
        val rdfs7 = instanceTriples.flatMap(t => spoBC.value.getOrElse(t.p, Set[String]()).map(supProp => RDFTriple(t.s, supProp, t.o)))

//        val rdfs7 = spo
//          .join(instanceTriples.alias("DATA"), $"SPO.s" === $"DATA.p", "inner")
//          .select($"DATA.s".alias("s"), $"SPO.o".alias("p"), $"DATA.s".alias("o"))
//          .as[RDFTriple]
//        instanceTriples = instanceTriples.union(rdfs7).cache()

        // rdfs2 (domain)
        var dom = if (tp.getObject.isConcrete) domain.filter(_.o == tp.getObject.toString()) else domain
        dom = dom.alias("DOM")
        var data = if (tp.getSubject.isConcrete) {
//          // asserted triples :s ?p ?o
//          val asserted = instanceTriples.filter(t => t.s == tp.getSubject.toString()).cache()
//          // join with super properties
//          val inferred = spo
//            .join(asserted.alias("DATA"), $"SPO.s" === $"DATA.p", "inner")
//            .select($"DATA.s".alias("s"), $"SPO.o".alias("p"), $"DATA.s".alias("o"))
//            .as[RDFTriple]
//          asserted.union(inferred)
          instanceTriples
        } else {
          instanceTriples
        }

        val rdftype = RDF.`type`.toString()

//        val rdfs2 = dom
//          .join(data.alias("DATA"), $"DOM.s" === $"DATA.p", "inner")
//          .select($"DATA.s", lit(RDF.`type`.toString).alias("p"), dom("o").alias("o"))
//          .as[RDFTriple]
        val domBC = session.sparkContext.broadcast(
          CollectionUtils.toMultiMap(dom.select("s", "o").collect().map(r => (r.getString(0), r.getString(1))))
        )
        val rdfs2 = data.flatMap(t => domBC.value.getOrElse(t.p, Set[String]()).map(o => RDFTriple(t.s, rdftype, o)))

        // rdfs3 (range)
        var ran = if (tp.getObject.isConcrete) range.filter(_.o == tp.getObject.toString()) else range
        ran = ran.alias("RAN")
        data = if (tp.getSubject.isConcrete) {
//          // asserted triples ?o ?p :s
//          val asserted = instanceTriples.filter(t => t.o == tp.getSubject.toString()).cache()
//          // join with super properties
//          val inferred = spo
//            .join(asserted.alias("DATA"), $"SPO.s" === $"DATA.p", "inner")
//            .select($"DATA.s".alias("s"), $"SPO.o".alias("p"), $"DATA.o".alias("o"))
//            .as[RDFTriple]
//          asserted.union(inferred)
          instanceTriples
        } else {
          instanceTriples
        }
//        val rdfs3 = ran
//          .join(data.alias("DATA"), $"RAN.s" === $"DATA.p", "inner")
//          .select($"DATA.o".alias("s"), lit(RDF.`type`.toString).alias("p"), ran("o").alias("o"))
//          .as[RDFTriple]
        val ranBC = session.sparkContext.broadcast(CollectionUtils.toMultiMap(ran.select("s", "o").collect().map(r => (r.getString(0), r.getString(1)))))
        val rdfs3 = data.flatMap(t => ranBC.value.getOrElse(t.p, Set[String]()).map(o => RDFTriple(t.o, rdftype, o)))

        // all rdf:type triples
        val types = rdfs2
          .union(rdfs3)
          .union(ds)
          .alias("TYPES")

        // rdfs9 (subClassOf)
        val scoTmp =
          if (tp.getObject.isURI) {
            sco.filter(_.o == tp.getObject.toString())
          } else {
            sco
          }
        val rdfs9 = scoTmp.alias("SCO")
          .join(types, $"SCO.s" === $"TYPES.o", "inner")
          .select(types("s").alias("s"), lit(RDF.`type`.toString()).alias("p"), sco("o").alias("o"))
          .as[RDFTriple]

//        log.info(s"|rdf:type|=${ds.count()}")
//        log.info(s"|rdfs2|=${rdfs2.count()}")
//        log.info(s"|rdfs3|=${rdfs3.count()}")
//        log.info(s"|rdf:type/rdfs2/rdfs3/|=${types.count()}")
//        log.info(s"|rdfs9|=${rdfs9.count()}")



        // add all rdf:type triples to result
        ds = ds
          .union(rdfs9)
          .union(types)
//          .repartition(200)

      } else if (tp.predicateMatches(RDFS.subClassOf.asNode())) {

      } else if (tp.predicateMatches(RDFS.subPropertyOf.asNode())) {

      } else { // instance data (s,p,o) with p!=rdf:type => we only have to cover subPropertyOf rule
        // filter instance data if subject or object was given
        val instanceData =
          if (tp.getSubject.isConcrete) {
              graph.filter(_.s == tp.getSubject.toString())
          } else if (tp.getObject.isConcrete) {
              graph.filter(_.o == tp.getObject.toString())
          } else {
              graph
          }

        // get all subproperties of p
        val subProperties = spo.filter(_.o == tp.getPredicate.toString()).alias("SPO")

        val rdfs7 = subProperties
          .join(instanceData.alias("DATA"), $"SPO.s" === $"DATA.p", "inner")
          .select($"DATA.s".alias("s"), $"SPO.o".alias("p"), $"DATA.o".alias("o"))
          .as[RDFTriple]

        ds = ds.union(rdfs7)
      }
    } else {

      val instanceData = ds

      if(tp.getSubject.isConcrete) {

        val tmp = spo
          .join(instanceData.alias("DATA"), $"SPO.s" === $"DATA.p", "inner")
          .select($"DATA.s".alias("s"), $"SPO.o".alias("p"), $"DATA.o".alias("o"))
          .as[RDFTriple]

        ds = ds.union(tmp)
      }
    }

//    ds.explain()

    ds.distinct()
  }

  /**
    * Computes the transitive closure for a Dataset of triples. The assumption is that this Dataset is already
    * filter by a single predicate.
    *
    * @param ds the Dataset of triples
    * @return a Dataset containing the transitive closure of the triples
    */
  private def computeTC(ds: Dataset[RDFTriple]): Dataset[RDFTriple] = {
    var tc = ds
    tc.cache()

    // the join is iterated until a fixed point is reached
    var i = 1
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      log.info(s"iteration $i...")
      oldCount = nextCount

      val joined = tc.alias("A")
                  .join(tc.alias("B"), $"A.o" === $"B.s")
                  .select($"A.s", $"A.p", $"B.o")
                  .as[RDFTriple]

      tc = tc
        .union(joined)
        .distinct()
        .cache()
      nextCount = tc.count()
      i += 1
    } while (nextCount != oldCount)

    tc.unpersist()

    log.info("TC has " + nextCount + " edges.")
    tc
  }
}

object BackwardChainingReasonerDataframe extends Logging{

  val DEFAULT_PARALLELISM = 200
  val DEFAULT_NUM_THREADS = 4

  def loadRDD(session: SparkSession, path: String): RDD[RDFTriple] = {
    RDFGraphLoader
      .loadFromDisk(session, path, 20)
      .triples.map(t => RDFTriple(t.getSubject.toString(), t.getPredicate.toString(), t.getObject.toString()))
  }

  def loadDataset(session: SparkSession, path: String): Dataset[RDFTriple] = {
    import session.implicits._
    session.createDataset(loadRDD(session, path))
  }

  def loadDatasetFromParquet(session: SparkSession, path: String): Dataset[RDFTriple] = {
    import session.implicits._
    session.read.parquet(path).as[RDFTriple]
  }

  def loadDataFrame(session: SparkSession, path: String): DataFrame = {
    loadDataset(session, path).toDF()
  }

  def loadDataFrameFromParquet(session: SparkSession, path: String): DataFrame = {
    loadDatasetFromParquet(session, path).toDF()
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) sys.error("USAGE: BackwardChainingReasonerDataframe <INPUT_PATH>+ <NUM_THREADS>? <PARALLELISM>?")

    val inputPath = args(0)
    val parquet = if (args.length > 1) args(1).toBoolean else false
    val numThreads = if (args.length > 2)  args(2).toInt else DEFAULT_NUM_THREADS
    val parallelism = if (args.length > 3)  args(3).toInt else DEFAULT_PARALLELISM

    // the SPARK config
    val session = SparkSession.builder
      .appName(s"Spark Backward Chaining")
      .master(s"local[$numThreads]")
            .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.shuffle.partitions", parallelism)
      .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        .config("parquet.enable.summary-metadata", "false")
//      .config("spark.sql.cbo.enabled", "true")path
//        .config("spark.local.dir", "/home/user/work/datasets/spark/tmp")
      .getOrCreate()

    var graph = if (parquet) loadDatasetFromParquet(session, inputPath) else loadDataset(session, inputPath)
    graph = graph.cache()
    graph.createOrReplaceTempView("TRIPLES")

    // compute size here to have it cached
    time {
      log.info(s"|G|=${graph.count()}")
    }

    val rules = RuleSets.RDFS_SIMPLE
      .filter(r => Seq(
        "rdfs2"
        , "rdfs3"
        , "rdfs9"
        , "rdfs7"

      ).contains(r.getName))

    val reasoner = new BackwardChainingReasonerDataframe(session, rules, graph)

    // VAR rdf:type URI
    var tp = Triple.create(
      NodeFactory.createVariable("s"),
      RDF.`type`.asNode(),
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person"))
    compare(tp, reasoner)

    // :s rdf:type VAR
    tp = Triple.create(
      NodeFactory.createURI("http://www.Department0.University0.edu/FullProfessor0"),
      RDF.`type`.asNode(),
      NodeFactory.createVariable("o"))
    compare(tp, reasoner)

    // :s rdfs:subClassOf VAR
    tp = Triple.create(
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#ClericalStaff"),
      RDFS.subClassOf.asNode(),
      NodeFactory.createVariable("o"))
    compare(tp, reasoner, true)

    // :s rdfs:subPropertyOf VAR
    tp = Triple.create(
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#headOf"),
      RDFS.subPropertyOf.asNode(),
      NodeFactory.createVariable("o"))
    compare(tp, reasoner)

    // VAR :p VAR
    tp = Triple.create(
      NodeFactory.createVariable("s"),
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#degreeFrom"),
      NodeFactory.createVariable("o"))
    compare(tp, reasoner)

    // :s :p VAR
    tp = Triple.create(
      NodeFactory.createURI("http://www.Department4.University3.edu/GraduateStudent40"),
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#degreeFrom"),
      NodeFactory.createVariable("o"))
    compare(tp, reasoner)

    // VAR :p :o
    tp = Triple.create(
      NodeFactory.createVariable("s"),
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#degreeFrom"),
      NodeFactory.createURI("http://www.University801.edu"))
    compare(tp, reasoner)

    // :s VAR :o
    tp = Triple.create(
      NodeFactory.createURI("http://www.Department4.University3.edu/GraduateStudent40"),
      NodeFactory.createVariable("p"),
      NodeFactory.createURI("http://www.University801.edu"))
    compare(tp, reasoner)

    // :s VAR VAR where :s is a resource
    tp = Triple.create(
      NodeFactory.createURI("http://www.Department4.University3.edu/GraduateStudent40"),
      NodeFactory.createVariable("p"),
      NodeFactory.createVariable("o"))
    compare(tp, reasoner)

    // :s VAR VAR where :s is a class
    tp = Triple.create(
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Book"),
      NodeFactory.createVariable("p"),
      NodeFactory.createVariable("o"))
    compare(tp, reasoner)

    // :s VAR VAR where :s is a property
    tp = Triple.create(
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#undergraduateDegreeFrom"),
      NodeFactory.createVariable("p"),
      NodeFactory.createVariable("o"))
    compare(tp, reasoner)

    session.stop()
  }

  def compare(tp: Triple, reasoner: BackwardChainingReasonerDataframe, show: Boolean = false): Unit = {
    time {
      val triples = reasoner.query(tp)
      println(triples.count())
      if (show) triples.show(false)
    }

//    time {
//      log.info(reasoner.isEntailed(tp))
//    }
  }

  import net.sansa_stack.inference.spark.utils.PrettyDuration._
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    log.info("Elapsed time: " + FiniteDuration(t1 - t0, "ns").pretty)
    result
  }
}

object TripleSchema extends StructType("s p o".split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false)))


