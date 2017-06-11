package net.sansa_stack.inference.rules.plan

import java.io.PrintWriter

import org.apache.calcite.config.Lex
import org.apache.calcite.plan.{Contexts, ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, Planner, RuleSets}
import collection.JavaConverters._
import scala.util.Try

import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.rules.{FilterJoinRule, ProjectJoinTransposeRule}
import org.apache.jena.reasoner.rulesys.Rule

import net.sansa_stack.inference.utils.RuleUtils

/**
  * @author Lorenz Buehmann
  */
class SimplePlanGenerator(schema: SchemaPlus) {

  val traitDefs: List[RelTraitDef[_ <: RelTrait]] = List(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)

  val optRuleSet = RuleSets.ofList(
    FilterJoinRule.FILTER_ON_JOIN,// push a filter into a join
    FilterJoinRule.JOIN,// push filter into the children of a join
    ProjectJoinTransposeRule.INSTANCE// push a projection to the children of a join
  )

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

    // parse to SQL node
    val sqlNode = Try(planner.parse(sqlQuery))

    // validate the SQL node
    val validatedSqlNode = planner.validate(sqlNode.get)

    // return the root node
    planner.rel(validatedSqlNode).project
  }

  /**
    * Generates a logical plan for the rules.
    *
    * @param rules the rules
    * @return the root node of the logical plan
    */
  def generateLogicalPlan(rules: Seq[Rule]): RelNode = {
    // generate SQL query
    val sqlQuery = rules.map(sqlGenerator.generateSQLQuery _).mkString("\tUNION \n")

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

    val plan = planGenerator.generateLogicalPlan(Seq(rules(0), rules(1)))

    plan.explain(new RelWriterImpl(new PrintWriter(System.out)))
  }
}
