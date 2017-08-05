package net.sansa_stack.inference.spark.backwardchaining

import net.sansa_stack.inference.rules.RuleSets
import net.sansa_stack.inference.rules.plan.{SimplePlanGenerator, SimpleSQLGenerator, TriplesSchema}
import net.sansa_stack.inference.spark.backwardchaining.tree.{AndNode, OrNode}
import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader
import net.sansa_stack.inference.utils.{Logging, TripleUtils}
import net.sansa_stack.inference.utils.RuleUtils._
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.reasoner.rulesys.impl.BindingVector
import org.apache.jena.vocabulary.RDF
import org.apache.spark.sql.{Dataset, SparkSession}
import net.sansa_stack.inference.utils.TripleUtils._


//case class RDFTriple(s: Node, p: Node, o: Node)
case class RDFTriple(s: String, p: String, o: String)

/**
  * @author Lorenz Buehmann
  */
class BackwardChainingReasonerDataframe(
                                         val session: SparkSession,
                                         val rules: Set[Rule],
                                         val graph: Dataset[RDFTriple]) extends Logging {

  def isEntailed(triple: Triple): Boolean = {
    isEntailed(new TriplePattern(triple))
  }

  def isEntailed(tp: TriplePattern): Boolean = {

    val tree = buildTree(new AndNode(tp), Seq())
    println(tree.toString)

    val triples = processTree(tree)
    triples.explain(true)
    println(triples.count())

    false
  }

  import org.apache.spark.sql.functions._
  val planGenerator = new SimplePlanGenerator(TriplesSchema.get())

  private def processTree(tree: AndNode): Dataset[RDFTriple] = {
    // 1. look for asserted triples in the graph
    val assertedTriples = lookup(tree.element)
    if(TripleUtils.isTerminological(tree.element.asTriple())) broadcast(assertedTriples)


    // 2. process the inference rules that can infer the triple pattern
    val inferredTriples = tree.children.map(child => {
      println(s"processing rule ${child.element}")

      // first process the children, i.e. we get the data for each triple pattern in the body of the rule
      val childrenTriples: Seq[Dataset[RDFTriple]] = child.children.map(processTree(_))

      val union = childrenTriples.reduce(_ union _)

      // then apply the rule on the UNION of the children data
      applyRule(child.element, union)
    })

    var triples = assertedTriples

    if(inferredTriples.nonEmpty) triples = triples.union(inferredTriples.reduce(_ union _))

    triples
  }

  private def lookup(tp: TriplePattern): Dataset[RDFTriple] = {
    val s = tp.getSubject.toString()
    val p = tp.getPredicate.toString()
    val o = tp.getObject.toString()
    var filteredGraph = graph
    if(tp.getSubject.isConcrete) {
      filteredGraph = filteredGraph.filter(t => t.s.equals(s))
    }
    if(tp.getPredicate.isConcrete) {
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
    val sql = sqlGenerator.generateSQLQuery(rule)
//    val sql =
//      """
//        |SELECT rel0.s, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' AS p, 'http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person' AS o
//        | FROM TRIPLES rel1 INNER JOIN TRIPLES rel0 ON rel1.s=rel0.p
//        | WHERE rel1.o='http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person' AND rel1.p='http://www.w3.org/2000/01/rdf-schema#domain'
//      """.stripMargin

    // generate logical execution plan
    val planGenerator = new SimplePlanGenerator(TriplesSchema.get())
    val plan = planGenerator.generateLogicalPlan(rule)

    dataset.sparkSession.sql(sql).as[RDFTriple]
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
      .getOrCreate()

    import session.implicits._
//    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[RDFTriple]

    val triples = RDFGraphLoader.loadFromDisk(session, args(0))
//      .triples.map(t => RDFTriple(t.getSubject, t.getPredicate, t.getObject))
      .triples.map(t => RDFTriple(t.getSubject.toString(), t.getPredicate.toString(), t.getObject.toString()))
    val graph = session.createDataset(triples).cache()
    graph.createOrReplaceTempView("TRIPLES")

    val rules = RuleSets.RDFS_SIMPLE
      .filter(r => Seq("rdfs2", "rdfs3").contains(r.getName))

    val tp = Triple.create(
      NodeFactory.createVariable("s"),
      RDF.`type`.asNode(),
      NodeFactory.createURI("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person"))

    val reasoner = new BackwardChainingReasonerDataframe(session, rules, graph)

    println(reasoner.isEntailed(tp))

    session.stop()
  }
}

