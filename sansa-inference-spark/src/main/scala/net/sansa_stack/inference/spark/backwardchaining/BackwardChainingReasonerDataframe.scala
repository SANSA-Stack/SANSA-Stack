package net.sansa_stack.inference.spark.backwardchaining

import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.reasoner.rulesys.impl.BindingVector
import org.apache.jena.sparql.util.FmtUtils
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.sql.{Dataset, SparkSession}

import net.sansa_stack.inference.rules.RuleSets
import net.sansa_stack.inference.rules.plan.SimpleSQLGenerator
import net.sansa_stack.inference.spark.backwardchaining.tree.{AndNode, OrNode}
import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader
import net.sansa_stack.inference.utils.RuleUtils._
import net.sansa_stack.inference.utils.{Logging, TripleUtils}


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

    val terminological = TripleUtils.isTerminological(tp.asTriple())

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


}

object BackwardChainingReasonerDataframe {


  def main(args: Array[String]): Unit = {

    val parallelism = 200

    // the SPARK config
    val session = SparkSession.builder
      .appName(s"Spark Backward Chaining")
      .master("local[4]")
      //      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.shuffle.partitions", parallelism)
      .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
      .getOrCreate()

    import session.implicits._
//    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[RDFTriple]

    val triples = RDFGraphLoader.loadFromDisk(session, args(0))
//      .triples.map(t => RDFTriple(t.getSubject, t.getPredicate, t.getObject))
      .triples.map(t => RDFTriple(t.getSubject.toString(), t.getPredicate.toString(), t.getObject.toString()))
    val graph = session.createDataset(triples).cache()
    graph.createOrReplaceTempView("TRIPLES")
    import org.apache.spark.sql.functions._
    val domain = graph.filter(t => t.p == RDFS.domain.toString)
    broadcast(domain).createOrReplaceTempView("DOMAIN")

    val query =
      """
        |SELECT rel0.s AS s, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' AS p, 'http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person' AS o
        | FROM DOMAIN rel1 JOIN TRIPLES rel0 ON rel1.s=rel0.p
        | WHERE rel1.o='http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person' AND rel1.p='http://www.w3.org/2000/01/rdf-schema#domain'
        | UNION
        | SELECT *
        | FROM TRIPLES
        | WHERE p ='http://www.w3.org/1999/02/22-rdf-syntax-ns#type' AND o ='http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person'
      """.stripMargin

    val ds = session.sql(query)
    ds.explain()
    println(ds.distinct().count())


    val rules = RuleSets.RDFS_SIMPLE
      .filter(r => Seq(
        "rdfs2"
//        , "rdfs3"
//        , "rdfs9"
      ).contains(r.getName))

    val tp = Triple.create(
      NodeFactory.createVariable("s"),
      RDF.`type`.asNode(),
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person"))

    val reasoner = new BackwardChainingReasonerDataframe(session, rules, graph)

    println(reasoner.isEntailed(tp))

    session.stop()


  }
}

