package net.sansa_stack.ml.spark.utils

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.jena.graph.{NodeFactory, Triple}


class SimilarityExperimentMetaGraphFactory {

  val spark = SparkSession.builder.getOrCreate()

  def transform(
               df: DataFrame
             )(
              metagraphExperimentName: String,
              metagraphExperimentType: String,
              metagraphExperimentMeasurementType: String
             )(
              metagraphElementRelation: String = "element",
              metagraphValueRelation: String = "value",
              metagraphExperimentTypeRelation: String = "experimentType",
              metagraphExperimentNameRelation: String = "experimentName",
              metagraphExperimentMeasurementTypeRelation: String = "experimentMeasurementType",
              metagraphExperimentDatetimeRelation: String = "experimentDatetime"
              ): RDD[Triple] = {

    val metagraphEvaluationDatetime = Calendar.getInstance().getTime()
      .toString // make string out of it, in future would be better to allow date nativly in rdf
      .replaceAll("\\s", "") // remove spaces to reduce confusions with some foreign file readers
      .replaceAll(":", "") // remove also double points for less confilcting chars.
    // create Central Node
    val metagraphExperimentHash: String = (metagraphExperimentType + metagraphExperimentName + metagraphEvaluationDatetime).replaceAll("\\s", "")
    // Create all inforamtion for this central node
    val centralNodeTriples = List(
      Triple.create(
        NodeFactory.createURI(metagraphExperimentHash),
        NodeFactory.createURI(metagraphExperimentDatetimeRelation),
        NodeFactory.createURI(metagraphEvaluationDatetime)
      ), Triple.create(
        NodeFactory.createURI(metagraphExperimentHash),
        NodeFactory.createURI(metagraphExperimentNameRelation),
        NodeFactory.createURI(metagraphExperimentName)
      ), Triple.create(
        NodeFactory.createURI(metagraphExperimentHash),
        NodeFactory.createURI(metagraphExperimentTypeRelation),
        NodeFactory.createLiteral(metagraphExperimentType)
      ), Triple.create(
        NodeFactory.createURI(metagraphExperimentHash),
        NodeFactory.createURI(metagraphExperimentMeasurementTypeRelation),
        NodeFactory.createLiteral(metagraphExperimentMeasurementType)
      )
    )

    val centralNodeRdd: RDD[Triple] = spark.sqlContext.sparkContext.parallelize(centralNodeTriples)

    // now for the small triples:
    val metagraph = df
      .rdd
      .flatMap(
        row => {
          val a: String = row(0).toString
          val b: String = row(1).toString
          val value: Double = row(2).toString.toDouble

          val simEstimationNode: String = metagraphExperimentHash + a + b

          List(
            Triple.create(
              NodeFactory.createURI(metagraphExperimentHash),
              NodeFactory.createURI(metagraphElementRelation), //This relation might to be changed in future
              NodeFactory.createURI(simEstimationNode)
            ), Triple.create(
              NodeFactory.createURI(simEstimationNode),
              NodeFactory.createURI(metagraphElementRelation),
              NodeFactory.createURI(a)
            ), Triple.create(
              NodeFactory.createURI(simEstimationNode),
              NodeFactory.createURI(metagraphElementRelation),
              NodeFactory.createURI(b)
            ), Triple.create(
              NodeFactory.createURI(simEstimationNode),
              NodeFactory.createURI(metagraphElementRelation),
              NodeFactory.createLiteral(value.toString) // TODO this should be double
            )
          )
        }
      )
      .union(centralNodeRdd)

    metagraph
  }

}
