package net.sansa_stack.query.spark.gremlinator.utils

import org.apache.commons.configuration.BaseConfiguration
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer

object Config {

  def getSparkConfig: BaseConfiguration = {
    val conf = new BaseConfiguration()
    conf.setProperty("spark.master", "local[*]")
    conf.setProperty("gremlin.spark.persistContext", "true")
    conf.setProperty("spark.serializer", "org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer")

    conf
  }

  def getSparkGraphComputer(connection: Graph): GraphComputer = {
    connection.compute(classOf[SparkGraphComputer])
      .result(GraphComputer.ResultGraph.NEW)
      .persist(GraphComputer.Persist.VERTEX_PROPERTIES)
  }

}