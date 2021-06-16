package net.sansa_stack.ml.spark.utils

import java.util.{Calendar, Date}

import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}


class SimilarityExperimentMetaGraphFactory {

  val spark = SparkSession.builder.getOrCreate()

  def createRdfOutput(outputDataset: Dataset[_])
                     (modelInformationEstimatorName: String, modelInformationEstimatorType: String, modelInformationMeasurementType: String)
                     (inputDatasetNumbertOfTriples: Long, dataSetInformationFilePath: String)
  : RDD[Triple] = {

    val metagraphDatetime: Date = Calendar.getInstance().getTime()
    val experimentID: String = "" +
      metagraphDatetime.toString.replaceAll("\\s", "").replaceAll(":", "") +
      modelInformationEstimatorName

    // create Literals
    val dateTimeLiteral: Node = NodeFactory.createLiteralByValue(metagraphDatetime.toString, XSDDatatype.XSDdateTime)// .createLiteralByValue(metagraphDatetime, XSDDateTimeType)
    val numberInputTriplesLiteral: Node = NodeFactory.createLiteralByValue(inputDatasetNumbertOfTriples.toString, XSDDatatype.XSDlong)
    val numberInputFilePathLiteral: Node = NodeFactory.createLiteral(dataSetInformationFilePath)
    val estimatorNameLiteral: Node = NodeFactory.createLiteral(modelInformationEstimatorName)
    val estimatorTypeLiteral: Node = NodeFactory.createLiteral(modelInformationEstimatorType)
    val measurementTypeLiteral: Node = NodeFactory.createLiteral(modelInformationMeasurementType)

    // create Properties
    // for central nodes
    val propertyCreatedDateTime: Node = NodeFactory.createURI("sansa-stack/sansaVocab/experimentDateTime")
    val propertyEstimatorName: Node = NodeFactory.createURI("sansa-stack/sansaVocab/estimatorName")
    val propertyEstimatorType: Node = NodeFactory.createURI("sansa-stack/sansaVocab/estimatorType")
    val propertyMeasurementType: Node = NodeFactory.createURI("sansa-stack/sansaVocab/measurementType")
    // for the similarity values
    val propertyEvaluated: Node = NodeFactory.createURI("sansa-stack/sansaVocab/evaluated")
    val propertyValue: Node = NodeFactory.createURI("sansa-stack/sansaVocab/evaluated")
    val propertyElement: Node = NodeFactory.createURI("sansa-stack/sansaVocab/element")

    // create central node for experiment
    val experimentIdNode: Node = NodeFactory.createBlankNode(experimentID)

    // Create all inforamtion for this central node
    val centralNodeTriples = List(
      Triple.create(
        experimentIdNode,
        propertyCreatedDateTime,
        dateTimeLiteral
      ), Triple.create(
        experimentIdNode,
        propertyEstimatorName,
        estimatorNameLiteral
      ), Triple.create(
        experimentIdNode,
        propertyEstimatorType,
        estimatorTypeLiteral
      ), Triple.create(
        experimentIdNode,
        propertyMeasurementType,
        measurementTypeLiteral
      )
    )

    val centralNodeRdd: RDD[Triple] = spark.sqlContext.sparkContext.parallelize(centralNodeTriples)

    // now for the small triples:
    val metagraph = outputDataset
      .rdd
      .flatMap(
        row => {
          val a: String = row.asInstanceOf[Row](0).toString
          val b: String = row.asInstanceOf[Row](1).toString
          val value: Double = row.asInstanceOf[Row](2).toString.toDouble

          val simEstimationNode: String = experimentID + a + b

          val nodeSimEst: Node = NodeFactory.createURI(simEstimationNode)
          val nodeA: Node = NodeFactory.createURI(a)
          val nodeB: Node = NodeFactory.createURI(b)

          val literalValue: Node = NodeFactory.createLiteralByValue(value.toString, XSDDatatype.XSDdouble)

          List(
            Triple.create(
              experimentIdNode,
              propertyEvaluated,
              nodeSimEst
            ), Triple.create(
              nodeSimEst,
              propertyElement,
              nodeA
            ), Triple.create(
              nodeSimEst,
              propertyElement,
              nodeB
            ), Triple.create(
              nodeSimEst,
              propertyValue,
              literalValue
            )
          )
        }
      )
      .union(centralNodeRdd)

    metagraph

  }

  // noinspection ScalaStyle
  /* def transform(
               df: DataFrame
             )(
              metagraphExperimentName: String,
              metagraphExperimentType: String,
              metagraphExperimentMeasurementType: String
             )(
              metagraphElementRelation: String = "element",
              metagraphEvaluatedRelation: String = "evaluated",
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
              NodeFactory.createURI(metagraphEvaluatedRelation), //This relation might to be changed in future
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
              NodeFactory.createLiteral(value.toString)
            )
          )
        }
      )
      .union(centralNodeRdd)

    metagraph
  } */

}
