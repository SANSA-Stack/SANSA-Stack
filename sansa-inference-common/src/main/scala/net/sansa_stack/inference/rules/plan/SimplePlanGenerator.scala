package net.sansa_stack.inference.rules.plan

import java.io.PrintWriter
import java.util.Collections

import com.google.common.collect.ImmutableList
import org.apache.calcite.config.Lex
import org.apache.calcite.plan._
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools._

import collection.JavaConverters._
import scala.util.Try
import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.rules._
import org.apache.jena.reasoner.rulesys.Rule
import net.sansa_stack.inference.utils.RuleUtils
import org.apache.calcite.adapter.enumerable.{EnumerableConvention, EnumerableRules}
import org.apache.calcite.interpreter.{BindableConvention, Bindables}
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgramBuilder}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.sql2rel.{RelDecorrelator, SqlToRelConverter}

/**
  * @author Lorenz Buehmann
  */
class SimplePlanGenerator(schema: SchemaPlus) {

  val traitDefs: List[RelTraitDef[_ <: RelTrait]] = List(
//    BindableConvention.INSTANCE.getTraitDef,
    ConventionTraitDef.INSTANCE
//    ,
    ,RelCollationTraitDef.INSTANCE
  )

  val optRuleSet = RuleSets.ofList(
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

  var program = Programs.ofRules(
    // push and merge filter rules
    FilterProjectTransposeRule.INSTANCE,
    FilterMergeRule.INSTANCE,
    FilterJoinRule.FILTER_ON_JOIN,
    FilterJoinRule.JOIN, /*push filter into the children of a join*/
    FilterTableScanRule.INSTANCE,
    // push and merge projection rules
    /*check the effectiveness of pushing down projections*/
    ProjectRemoveRule.INSTANCE,
    ProjectJoinTransposeRule.INSTANCE,
    //JoinProjectTransposeRule.BOTH_PROJECT,
    //ProjectFilterTransposeRule.INSTANCE, /*it is better to use filter first an then project*/
    ProjectTableScanRule.INSTANCE,
    ProjectWindowTransposeRule.INSTANCE,
    ProjectMergeRule.INSTANCE,
    //join rules
    /*A simple trick is to consider a window size equal to stream cardinality.
     * For tuple-based windows, the window size is equal to the number of tuples.
     * For time-based windows, the window size is equal to the (input_rate*time_of_window).*/
    //JoinToMultiJoinRule.INSTANCE ,
    //LoptOptimizeJoinRule.INSTANCE ,
    //MultiJoinOptimizeBushyRule.INSTANCE,
    JoinPushThroughJoinRule.RIGHT,
    JoinPushThroughJoinRule.LEFT, /*choose between right and left*/
    JoinPushExpressionsRule.INSTANCE,
    JoinAssociateRule.INSTANCE
  )
  program = Programs.ofRules(Bindables.RULES)

  val calciteFrameworkConfig: FrameworkConfig =
    Frameworks.newConfigBuilder
      .parserConfig(
        SqlParser.configBuilder
          // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
          .setLex(Lex.MYSQL)
          .build)
      // Sets the schema to use by the planner
      .defaultSchema(schema)
      .traitDefs(traitDefs.asJava)
      // Context provides a way to store data within the planner session that can be accessed in planner rules.
      .context(Contexts.EMPTY_CONTEXT)
      // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
      .ruleSets(optRuleSet)
      // Custom cost factory to use during optimization
      .costFactory(null)
//      .programs(program)
      .typeSystem(RelDataTypeSystem.DEFAULT)
      .build()

  lazy val planner: Planner = Frameworks.getPlanner(calciteFrameworkConfig)

  val sqlGenerator = new SimpleSQLGenerator

  /**
    * Generates a logical plan for the rule.
    *
    * @param rule the rule
    * @return the root node of the logical plan
    */
  def generateLogicalPlan(rule: Rule): RelNode = {
    // generate SQL query
    val sqlQuery = sqlGenerator.generateSQLQuery(rule)
    println(sqlQuery)

    // parse to SQL node
    val sqlNode = Try(planner.parse(sqlQuery))

    // validate the SQL node
    val validatedSqlNode = planner.validate(sqlNode.get)

    // return the root node
    var root = planner.rel(validatedSqlNode).project() // .project
//    root.register(basePlanner)
//
//
//    basePlanner.setRoot(root)
//    optimizePlan(root)

    import org.apache.calcite.adapter.enumerable.EnumerableConvention
    import org.apache.calcite.plan.RelTraitSet
    val desiredTraits = root.getTraitSet.replace(EnumerableConvention.INSTANCE).simplify()

//    root = root.getCluster.getPlanner.chooseDelegate.changeTraits(root, desiredTraits)

    val node = planner.transform(0,
      root.getTraitSet.simplify().plus(BindableConvention.INSTANCE),
//      planner.getEmptyTraitSet
//        .replace(ConventionTraitDef.INSTANCE.getDefault)
//        .replace(RelCollationTraitDef.INSTANCE.getDefault),
      root)
    planner.reset()
    node
  }

  val DATASET_NORM_RULES: RuleSet = RuleSets.ofList(
    // simplify expressions rules
    ReduceExpressionsRule.FILTER_INSTANCE,
    ReduceExpressionsRule.PROJECT_INSTANCE,
    ReduceExpressionsRule.CALC_INSTANCE,
    ReduceExpressionsRule.JOIN_INSTANCE,
    ProjectToWindowRule.PROJECT
  )

  val DATASET_OPT_RULES: RuleSet = RuleSets.ofList(

    // convert a logical table scan to a relational expression
    TableScanRule.INSTANCE,
//    EnumerableToLogicalTableScan.INSTANCE,

    // push a filter into a join
    FilterJoinRule.FILTER_ON_JOIN,
    // push filter into the children of a join
    FilterJoinRule.JOIN,
    // push filter through an aggregation
    FilterAggregateTransposeRule.INSTANCE,

    // aggregation and projection rules
    AggregateProjectMergeRule.INSTANCE,
    AggregateProjectPullUpConstantsRule.INSTANCE,
    // push a projection past a filter or vice versa
    ProjectFilterTransposeRule.INSTANCE,
    FilterProjectTransposeRule.INSTANCE,
    // push a projection to the children of a join
    ProjectJoinTransposeRule.INSTANCE,
    // remove identity project
    ProjectRemoveRule.INSTANCE,
    // reorder sort and projection
    SortProjectTransposeRule.INSTANCE,
    ProjectSortTransposeRule.INSTANCE,

    // join rules
    JoinPushExpressionsRule.INSTANCE,

    // remove union with only a single child
    UnionEliminatorRule.INSTANCE,
    // convert non-all union into all-union + distinct
    UnionToDistinctRule.INSTANCE,

    // remove aggregation if it does not aggregate and input is already distinct
    AggregateRemoveRule.INSTANCE,
    // push aggregate through join
    AggregateJoinTransposeRule.EXTENDED,
    // aggregate union rule
    AggregateUnionAggregateRule.INSTANCE,
    // expand distinct aggregate to normal aggregate with groupby
    AggregateExpandDistinctAggregatesRule.JOIN,

    // remove unnecessary sort rule
    SortRemoveRule.INSTANCE,

    // prune empty results rules
    PruneEmptyRules.AGGREGATE_INSTANCE,
    PruneEmptyRules.FILTER_INSTANCE,
    PruneEmptyRules.JOIN_LEFT_INSTANCE,
    PruneEmptyRules.JOIN_RIGHT_INSTANCE,
    PruneEmptyRules.PROJECT_INSTANCE,
    PruneEmptyRules.SORT_INSTANCE,
    PruneEmptyRules.UNION_INSTANCE,

    // calc rules
    FilterCalcMergeRule.INSTANCE,
    ProjectCalcMergeRule.INSTANCE,
    FilterToCalcRule.INSTANCE,
    ProjectToCalcRule.INSTANCE,
    CalcMergeRule.INSTANCE

  )

  def optimizePlan(relNode: RelNode): RelNode = {

    def runHepPlanner(
                       hepMatchOrder: HepMatchOrder,
                       ruleSet: RuleSet,
                       input: RelNode,
                       targetTraits: RelTraitSet): RelNode = {
      val builder = new HepProgramBuilder
      builder.addMatchOrder(hepMatchOrder)

      val it = ruleSet.iterator()
      while (it.hasNext) {
        builder.addRuleInstance(it.next())
      }

      val planner = new HepPlanner(builder.build, calciteFrameworkConfig.getContext)
      planner.setRoot(input)
      if (input.getTraitSet != targetTraits) {
        planner.changeTraits(input, targetTraits.simplify)
      }
      planner.findBestExp
    }

    def runVolcanoPlanner(
                                     ruleSet: RuleSet,
                                     input: RelNode,
                                     targetTraits: RelTraitSet): RelNode = {
      val optProgram = Programs.ofRules(ruleSet)

      val output = try {
        optProgram.run(basePlanner, input, targetTraits,
          ImmutableList.of(), ImmutableList.of())
      } catch {
        case e: Exception =>
          throw e
      }
      output
    }

    // 1. decorrelate
    val decorPlan = RelDecorrelator.decorrelateQuery(relNode)

    // 2. normalize the logical plan
    val normRuleSet = DATASET_NORM_RULES
    val normalizedPlan = if (normRuleSet.iterator().hasNext) {
      runHepPlanner(HepMatchOrder.BOTTOM_UP, normRuleSet, decorPlan, decorPlan.getTraitSet)
    } else {
      decorPlan
    }

    // 3. optimize the logical Flink plan
    val optRuleSet = DATASET_OPT_RULES
    val flinkOutputProps = relNode.getTraitSet.replace(ConventionTraitDef.INSTANCE.getDefault).simplify()
    val optimizedPlan = if (optRuleSet.iterator().hasNext) {
      runVolcanoPlanner(optRuleSet, normalizedPlan, flinkOutputProps)
    } else {
      normalizedPlan
    }

    optimizedPlan
  }

  val basePlanner: RelOptPlanner = {
    val planner = new VolcanoPlanner(calciteFrameworkConfig.getCostFactory, Contexts.empty())
    planner.setExecutor(calciteFrameworkConfig.getExecutor)
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
    planner
  }

  def plan(rule: Rule) = {
//    // generate SQL query
//    val sqlQuery = sqlGenerator.generateSQLQuery(rule)
//
//    val sqlParser = SqlParser.create(sqlQuery, SqlParser.configBuilder().build())
//    val sqlNode = sqlParser.parseStmt()
//    val catalogReader = null
//
//    val pl = new VolcanoPlanner()
//
//    val sqlToRelConverter = new SqlToRelConverter(new ViewExpanderImpl(), )
//
//    program.run(pl, , planner.getEmptyTraitSet, Collections.emptyList(), Collections.emptyList())

  }

  /**
    * Generates a logical plan for the rules.
    *
    * @param rules the rules
    * @return the root node of the logical plan
    */
  def generateLogicalPlan(rules: Seq[Rule]): RelNode = {
    planner.close()
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
    planner.rel(validatedSqlNode).project
  }
}

object SimplePlanGenerator {

  def main(args: Array[String]): Unit = {

    val planGenerator = new SimplePlanGenerator(TriplesSchema.get())

    val rules = RuleUtils.load("rules/rdfs-simple.rules")
    rules.foreach(rule => {
      println(rule)
      planGenerator.generateLogicalPlan(rule).explain(new RelWriterImpl(new PrintWriter(System.out)))
    })

    val plan = planGenerator.generateLogicalPlan(Seq(rules(0), rules(1)))

    plan.explain(new RelWriterImpl(new PrintWriter(System.out)))
  }
}
