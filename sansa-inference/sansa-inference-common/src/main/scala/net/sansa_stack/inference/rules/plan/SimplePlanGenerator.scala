package net.sansa_stack.inference.rules.plan

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.calcite.config.Lex
import org.apache.calcite.interpreter.{BindableConvention, Bindables}
import org.apache.calcite.plan.{RelOptUtil, _}
import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.rel.rules._
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools._
import org.apache.jena.reasoner.rulesys.Rule

import net.sansa_stack.inference.utils.{Logging, RuleUtils}

/**
  * @author Lorenz Buehmann
  */
class SimplePlanGenerator(schema: SchemaPlus) extends Logging {

  val traitDefs: List[RelTraitDef[_ <: RelTrait]] = List(
    ConventionTraitDef.INSTANCE,
    RelCollationTraitDef.INSTANCE,
    BindableConvention.INSTANCE.getTraitDef
  )

  val optRuleSet: RuleSet = RuleSets.ofList(
    FilterJoinRule.FILTER_ON_JOIN,// push a filter into a join
    FilterJoinRule.JOIN,// push filter into the children of a join
    ProjectJoinTransposeRule.INSTANCE// push a projection to the children of a join
//    ,
//    // push and merge filter rules
//    FilterAggregateTransposeRule.INSTANCE,
//    FilterProjectTransposeRule.INSTANCE,
//    FilterMergeRule.INSTANCE,
//    FilterJoinRule.FILTER_ON_JOIN,
//    FilterJoinRule.JOIN, /*push filter into the children of a join*/
//    FilterTableScanRule.INSTANCE,
//    // push and merge projection rules
//    /*check the effectiveness of pushing down projections*/
//    ProjectRemoveRule.INSTANCE,
//    ProjectJoinTransposeRule.INSTANCE
  )

  val calciteFrameworkConfig: FrameworkConfig =
//    Frameworks.newConfigBuilder
//      .parserConfig(
//        SqlParser.configBuilder
//          // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
//          .setLex(Lex.MYSQL)
//          .build)
//      // Sets the schema to use by the planner
//      .defaultSchema(schema)
//      .traitDefs(traitDefs.asJava)
//      // Context provides a way to store data within the planner session that can be accessed in planner rules.
//      .context(Contexts.EMPTY_CONTEXT)
//      // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
// //      .ruleSets(optRuleSet)
//      .ruleSets(RuleSets.ofList(Bindables.BINDABLE_TABLE_SCAN_RULE, Bindables.BINDABLE_PROJECT_RULE, Bindables.BINDABLE_JOIN_RULE, Bindables.BINDABLE_FILTER_RULE, FilterJoinRule.FILTER_ON_JOIN))
//      .programs(Programs.ofRules(Bindables.BINDABLE_TABLE_SCAN_RULE, Bindables.BINDABLE_PROJECT_RULE, Bindables.BINDABLE_JOIN_RULE, Bindables.BINDABLE_FILTER_RULE, FilterJoinRule.FILTER_ON_JOIN))
//
//      // Custom cost factory to use during optimization
//      .costFactory(null)
// //      .programs(program)
//      .typeSystem(RelDataTypeSystem.DEFAULT)
//      .build()

  Frameworks.newConfigBuilder
    .parserConfig(SqlParser.configBuilder.setLex(Lex.MYSQL).build)
    .defaultSchema(schema) // Sets the schema to use by the planner
    .traitDefs(traitDefs.asJava)
    .context(Contexts.EMPTY_CONTEXT)
    .ruleSets(RuleSets.ofList(Bindables.BINDABLE_TABLE_SCAN_RULE, Bindables.BINDABLE_PROJECT_RULE, Bindables.BINDABLE_JOIN_RULE, Bindables.BINDABLE_FILTER_RULE, FilterJoinRule.FILTER_ON_JOIN))
    .programs(Programs.ofRules(Bindables.BINDABLE_TABLE_SCAN_RULE, Bindables.BINDABLE_PROJECT_RULE, Bindables.BINDABLE_JOIN_RULE, Bindables.BINDABLE_FILTER_RULE, FilterJoinRule.FILTER_ON_JOIN))
    .costFactory(null)
    .typeSystem(RelDataTypeSystem.DEFAULT)
    .build

  lazy val planner: Planner = Frameworks.getPlanner(calciteFrameworkConfig)

  val sqlGenerator = new SimpleSQLGenerator

  /**
    * Generates a logical plan for the rule.
    *
    * @param rule the rule
    * @param optimized whether to get an optimized plan based on Apache Calcite transformation rules
    * @return the root node of the logical plan
    */
  def generateLogicalPlan(rule: Rule, optimized: Boolean = true): RelNode = {
    // generate SQL query
    val sqlQuery = sqlGenerator.generateSQLQuery(rule)
    log.debug(s"SQL Query: $sqlQuery")

    // parse to SQL node
    val sqlNode = Try(planner.parse(sqlQuery))
    if(sqlNode.isFailure) throw new RuntimeException("Failed to parse SQL query to SQL node. Please check syntax")

    // validate the SQL node
    val validatedSqlNode = planner.validate(sqlNode.get)

    // return the root node
    var plan = planner.rel(validatedSqlNode).project
    log.debug(s"Query plan: ${RelOptUtil.toString(plan)}")

    // compute optimized plan if enabled
    if (optimized) {
      plan = getOptimizedPlan(plan)
      log.debug(s"Optimized Query plan: ${RelOptUtil.toString(plan)}")
    }

    // close and reset between each rule resp. query
    planner.close()
    planner.reset()

    plan
  }

  private def getOptimizedPlan(plan: RelNode): RelNode = {
    var traitSet: RelTraitSet = plan.getTraitSet
    traitSet = traitSet.simplify()
    traitSet = planner.getEmptyTraitSet

    val optimizedPlan = planner.transform(0, traitSet.plus(BindableConvention.INSTANCE), plan)
    optimizedPlan
  }

  /**
    * Generates a logical plan for the rules.
    *
    * @param rules the rules
    * @return the root node of the logical plan
    */
  def generateLogicalPlan(rules: Seq[Rule]): RelNode = {
    // generate SQL query
//    val sqlQuery = rules.map(sqlGenerator.generateSQLQuery _).mkString("\tUNION \n")
//    val sqlQuery = " select * from triples a, triples b where a.s='foo' and b.s='foo' "
    val sqlQuery =
      """
        |SELECT rel0.s, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', rel1.o
        | FROM TRIPLES rel1 INNER JOIN TRIPLES rel0 ON rel1.s=rel0.s
        | WHERE rel1.p='http://www.w3.org/2000/01/rdf-schema#domain'
      """.stripMargin

    // parse to SQL node
    val sqlNode = Try(planner.parse(sqlQuery))

    // validate the SQL node
    val validatedSqlNode = planner.validate(sqlNode.get)

    // return the root node
    val plan = planner.rel(validatedSqlNode).project
    planner.reset()

    plan
  }
}

object SimplePlanGenerator {

  def main(args: Array[String]): Unit = {

    val planGenerator = new SimplePlanGenerator(TriplesSchema.get())

    val rules = RuleUtils.load("rules/rdfs-simple.rules")
    rules.foreach(rule => {
      println(rule)
      val plan = planGenerator.generateLogicalPlan(rule)
      println(RelOptUtil.toString(plan))
    })
  }
}
