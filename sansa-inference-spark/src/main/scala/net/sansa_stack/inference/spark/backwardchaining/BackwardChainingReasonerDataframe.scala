package net.sansa_stack.inference.spark.backwardchaining


import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.reasoner.rulesys.impl.BindingVector
import org.apache.jena.sparql.util.FmtUtils
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import net.sansa_stack.inference.rules.RuleSets
import net.sansa_stack.inference.rules.plan.SimpleSQLGenerator
import net.sansa_stack.inference.spark.backwardchaining.BackwardChainingReasonerDataframe.time
import net.sansa_stack.inference.spark.backwardchaining.tree.{AndNode, OrNode}
import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader
import net.sansa_stack.inference.utils.RuleUtils._
import net.sansa_stack.inference.utils.{Logging, TripleUtils}

import scala.concurrent.duration.FiniteDuration


//case class RDFTriple(s: Node, p: Node, o: Node)
case class RDFTriple(s: String, p: String, o: String)

/**
  * @author Lorenz Buehmann
  */
class BackwardChainingReasonerDataframe(
                                         val session: SparkSession,
                                         val rules: Set[Rule],
                                         val graph: Dataset[RDFTriple]) extends Logging {

  import org.apache.spark.sql.functions._

  val precomputeSchema: Boolean = true

  var schema: Map[Node, Dataset[RDFTriple]] = Map()

  def isEntailed(triple: Triple): Boolean = {
    isEntailed(new TriplePattern(triple))
  }

  def isEntailed(tp: TriplePattern): Boolean = {

    if (precomputeSchema) schema = extractWithIndex(graph)

    val tree = buildTree(new AndNode(tp), Seq())
    println(tree.toString)

    val triples = processTree(tree)
    triples.explain(true)
    println(triples.distinct().count())

    false
  }



  private def processTree(tree: AndNode): Dataset[RDFTriple] = {
    // 1. look for asserted triples in the graph
    val assertedTriples = lookup(tree.element)
    if(TripleUtils.isTerminological(tree.element.asTriple())) broadcast(assertedTriples)


    // 2. process the inference rules that can infer the triple pattern
    val inferredTriples = tree.children.map(child => {
      println(s"processing rule ${child.element}")

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
      var head = r.headTriplePatterns()

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
    println(s"SQL NEW: $sql")
    dataset.createOrReplaceTempView(tableName)
    dataset.sparkSession.sql(sql).as[RDFTriple]
  }

  val properties = Set(RDFS.subClassOf, RDFS.subPropertyOf, RDFS.domain, RDFS.range).map(p => p.asNode())
  val DUMMY_VAR = NodeFactory.createVariable("VAR");

  /**
    * Computes the triples for each schema property p, e.g. `rdfs:subClassOf` and returns it as mapping from p
    * to the [[Dataset]] containing the triples.
    *
    * @param graph the RDF graph
    * @return a mapping from the corresponding schema property to the Dataframe of s-o pairs
    */
  def extractWithIndex(graph: Dataset[RDFTriple]): Map[Node, Dataset[RDFTriple]] = {
    log.info("Started schema extraction...")

    // for each schema property p
    val index =
      properties.map { p =>
        // get triples (s, p, o)
        var triples = lookup(new TriplePattern(DUMMY_VAR, p, DUMMY_VAR))

        // broadcast the triples
        triples = broadcast(triples)

        // register as a table
        triples.createOrReplaceTempView(FmtUtils.stringForNode(p).replace(":", "_"))

        // add to index
        (p -> triples)
      }
    log.info("Finished schema extraction.")

    index.toMap
  }

  def query(tp: Triple): Dataset[RDFTriple] = {
    import org.apache.spark.sql.functions._

    val domain = broadcast(graph.filter(t => t.p == RDFS.domain.toString)).alias("DOMAIN")
    domain.createOrReplaceTempView("DOMAIN")

    val range = broadcast(graph.filter(t => t.p == RDFS.range.toString)).alias("RANGE")
    range.createOrReplaceTempView("RANGE")

    val sco = broadcast(graph.filter(t => t.p == RDFS.subClassOf.toString)).alias("SCO")
    sco.createOrReplaceTempView("SCO")

    val spo = broadcast(graph.filter(t => t.p == RDFS.subPropertyOf.toString)).alias("SPO")
    spo.createOrReplaceTempView("SPO")

    // asserted triples
    var ds = lookup(tp)

    // inferred triples
    if(tp.getPredicate.isConcrete) {

      if (tp.getPredicate.matches(RDF.`type`.asNode())) { // rdf:type data

        // get all non rdf:type triples first
        var instanceTriples = graph.filter(_.p != RDF.`type`.toString)

        // enrich the instance data with super properties, i.e. rdfs5
        if(tp.getSubject.isConcrete) { // find triples where s occurs as subject or object
          instanceTriples = instanceTriples.filter(t => t.s == tp.getSubject.toString() || t.o == tp.getSubject.toString())
        }
        val rdfs7 = spo
          .join(instanceTriples.alias("DATA"), $"SPO.s" === $"DATA.p", "inner")
          .select($"DATA.s".alias("s"), $"SPO.o".alias("p"), $"DATA.s".alias("o"))
          .as[RDFTriple]
        instanceTriples = instanceTriples.union(rdfs7).cache()

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
        val rdfs2 = dom
          .join(data.alias("DATA"), $"DOM.s" === $"DATA.p", "inner")
          .select($"DATA.s", lit(RDF.`type`.toString).alias("p"), dom("o").alias("o"))
          .as[RDFTriple]

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
        val rdfs3 = ran
          .join(data.alias("DATA"), $"RAN.s" === $"DATA.p", "inner")
          .select($"DATA.o".alias("s"), lit(RDF.`type`.toString).alias("p"), ran("o").alias("o"))
          .as[RDFTriple]

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
          .select(types("s").alias("s"), lit(RDF.`type`.toString).alias("p"), sco("o").alias("o"))
          .as[RDFTriple]

//        println(s"|rdf:type|=${ds.count()}")
//        println(s"|rdfs2|=${rdfs2.count()}")
//        println(s"|rdfs3|=${rdfs3.count()}")
//        println(s"|rdf:type/rdfs2/rdfs3/|=${types.count()}")
//        println(s"|rdfs9|=${rdfs9.count()}")



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


}

object BackwardChainingReasonerDataframe {


  def main(args: Array[String]): Unit = {

    val parallelism = 200

    // the SPARK config
    val session = SparkSession.builder
      .appName(s"Spark Backward Chaining")
      .master("local[4]")
            .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.shuffle.partitions", parallelism)
      .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        .config("parquet.enable.summary-metadata", "false")
        .config("spark.local.dir", "/home/user/work/datasets/spark/tmp")
      .getOrCreate()

    import session.implicits._
//    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[RDFTriple]

    val path = "/home/user/work/datasets/lubm/1000/univ-bench.nt"//args(0)

//    val triples = RDFGraphLoader.loadFromDisk(session, path)
//      .triples.map(t => RDFTriple(t.getSubject.toString(), t.getPredicate.toString(), t.getObject.toString()))
////      .triples.map(t => RDFTriple(FmtUtils.stringForNode(t.getSubject), FmtUtils.stringForNode(t.getPredicate), FmtUtils.stringForNode(t.getObject)))
//
//    val tableDir = "/home/user/work/datasets/lubm/table/1000"
//    val graph = session.createDataset(triples)//.cache()
//    graph.write.mode(SaveMode.Append).parquet(tableDir)

    val graph = session.read.parquet(args(0)).as[RDFTriple].cache()
    graph.createOrReplaceTempView("TRIPLES")

    // compute size here to have it cached
    time {
      println(s"|G|=${graph.count()}")
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

//    // :s rdf:type VAR
//    tp = Triple.create(
//      NodeFactory.createURI("http://www.Department0.University0.edu/FullProfessor0"),
//      RDF.`type`.asNode(),
//      NodeFactory.createVariable("o"))
//    compare(tp, reasoner)
//
//    // VAR :p VAR
//    tp = Triple.create(
//      NodeFactory.createVariable("s"),
//      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#degreeFrom"),
//      NodeFactory.createVariable("o"))
//    compare(tp, reasoner)
//
//    // :s :p VAR
//    tp = Triple.create(
//      NodeFactory.createURI("http://www.Department4.University3.edu/GraduateStudent40"),
//      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#degreeFrom"),
//      NodeFactory.createVariable("o"))
//    compare(tp, reasoner)
//
//    // VAR :p :o
//    tp = Triple.create(
//      NodeFactory.createVariable("s"),
//      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#degreeFrom"),
//      NodeFactory.createURI("http://www.University801.edu"))
//    compare(tp, reasoner)
//
//    // :s VAR :o
//    tp = Triple.create(
//      NodeFactory.createURI("http://www.Department4.University3.edu/GraduateStudent40"),
//      NodeFactory.createVariable("p"),
//      NodeFactory.createURI("http://www.University801.edu"))
//    compare(tp, reasoner)
//
//    // :s VAR VAR where :s is a resource
//    tp = Triple.create(
//      NodeFactory.createURI("http://www.Department4.University3.edu/GraduateStudent40"),
//      NodeFactory.createVariable("p"),
//      NodeFactory.createVariable("o"))
//    compare(tp, reasoner)
//
//    // :s VAR VAR where :s is a class
//    tp = Triple.create(
//      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Book"),
//      NodeFactory.createVariable("p"),
//      NodeFactory.createVariable("o"))
//    compare(tp, reasoner)
//
//    // :s VAR VAR where :s is a property
//    tp = Triple.create(
//      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#undergraduateDegreeFrom"),
//      NodeFactory.createVariable("p"),
//      NodeFactory.createVariable("o"))
//    compare(tp, reasoner)

    session.stop()
  }

  def compare(tp: Triple, reasoner: BackwardChainingReasonerDataframe): Unit = {
    time {
      val triples = reasoner.query(tp)
      println(triples.count())
//      println(triples.show(false))
    }

//    time {
//      println(reasoner.isEntailed(tp))
//    }
  }

  import PrettyDuration._
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + FiniteDuration(t1 - t0, "ns").pretty)
    result
  }
}

object PrettyDuration {

  import scala.concurrent.duration._

  implicit class PrettyPrintableDuration(val duration: Duration) extends AnyVal {

    def pretty: String = pretty(includeNanos = false)

    /** Selects most apropriate TimeUnit for given duration and formats it accordingly */
    def pretty(includeNanos: Boolean, precision: Int = 4): String = {
      require(precision > 0, "precision must be > 0")

      duration match {
        case d: FiniteDuration =>
      val nanos = d.toNanos
      val unit = chooseUnit(nanos)
      val value = nanos.toDouble / NANOSECONDS.convert(1, unit)

      s"%.${precision}g %s%s".format(value, abbreviate(unit), if (includeNanos) s" ($nanos ns)" else "")

        case d: Duration.Infinite if d == Duration.MinusInf  =>  s" -âˆž (minus infinity)"
        case d  =>  s"âˆž (infinity)"
      }
    }

    def chooseUnit(nanos: Long): TimeUnit = {
      val d = nanos.nanos

      if (d.toDays > 0) DAYS
      else if (d.toHours > 0) HOURS
      else if (d.toMinutes > 0) MINUTES
      else if (d.toSeconds > 0) SECONDS
      else if (d.toMillis > 0) MILLISECONDS
      else if (d.toMicros > 0) MICROSECONDS
      else NANOSECONDS
    }

    def abbreviate(unit: TimeUnit): String = unit match {
      case NANOSECONDS  => "ns"
      case MICROSECONDS  => "Î¼s"
      case MILLISECONDS  => "ms"
      case SECONDS       => "s"
      case MINUTES       => "min"
      case HOURS         => "h"
      case DAYS          => "d"
    }
  }

}

