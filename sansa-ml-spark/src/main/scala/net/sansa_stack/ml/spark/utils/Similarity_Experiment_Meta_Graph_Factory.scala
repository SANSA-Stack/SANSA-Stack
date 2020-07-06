package net.sansa_stack.ml.spark.utils

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.jena.graph.{NodeFactory, Triple}


class Similarity_Experiment_Meta_Graph_Factory {

  val spark = SparkSession.builder.getOrCreate()

  def transform(
               df: DataFrame
             )(
              metagraph_experiment_name: String,
              metagraph_experiment_type: String,
              metagraph_experiment_measurement_type: String
             )(
              metagraph_element_relation: String = "element",
              metagraph_value_relation: String = "value",
              metagraph_experiment_type_relation: String = "experiment_type",
              metagraph_experiment_name_relation: String = "experiment_name",
              metagraph_experiment_measurement_type_relation: String = "experiment_measurement_type",
              metagraph_experiment_datetime_relation: String = "experiment_datetime"
              ): RDD[Triple] = {

    val metagraph_evaluation_datetime = Calendar.getInstance().getTime()
      .toString // make string out of it, in future would be better to allow date nativly in rdf
      .replaceAll("\\s", "") // remove spaces to reduce confusions with some foreign file readers
      .replaceAll(":", "") // remove also double points for less confilcting chars.
    // create Central Node
    val metagraph_experiment_hash: String = (metagraph_experiment_type + metagraph_experiment_name + metagraph_evaluation_datetime).replaceAll("\\s", "")
    // Create all inforamtion for this central node
    val central_node_triples = List(
      Triple.create(
        NodeFactory.createURI(metagraph_experiment_hash),
        NodeFactory.createURI(metagraph_experiment_datetime_relation),
        NodeFactory.createURI(metagraph_evaluation_datetime)
      ), Triple.create(
        NodeFactory.createURI(metagraph_experiment_hash),
        NodeFactory.createURI(metagraph_experiment_name_relation),
        NodeFactory.createURI(metagraph_experiment_name)
      ), Triple.create(
        NodeFactory.createURI(metagraph_experiment_hash),
        NodeFactory.createURI(metagraph_experiment_type_relation),
        NodeFactory.createLiteral(metagraph_experiment_type)
      ), Triple.create(
        NodeFactory.createURI(metagraph_experiment_hash),
        NodeFactory.createURI(metagraph_experiment_measurement_type_relation),
        NodeFactory.createLiteral(metagraph_experiment_measurement_type)
      )
    )
    // TODO now for each line also the other triples to be created
    spark.sqlContext.sparkContext.parallelize(central_node_triples)
  }

}
