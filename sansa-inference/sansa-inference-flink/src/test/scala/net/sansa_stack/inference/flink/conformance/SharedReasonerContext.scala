package net.sansa_stack.inference.flink.conformance

import net.sansa_stack.inference.flink.forwardchaining.ForwardRuleReasoner
import net.sansa_stack.kryo.jena.{DefaultNodeSerializer, TripleSerializer}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.jena.graph.{Node, Triple}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * A shared reasoner and Flink environment used for multiple test cases using the same resources.
  *
  * @author Lorenz Buehmann
  */
trait SharedReasonerContext[R <: ForwardRuleReasoner]
  extends BeforeAndAfterAll
    with ReasonerContextProvider {
  self: Suite =>

  @transient protected var _reasoner: R = _
  def reasoner: R = _reasoner

  @transient private var _env: ExecutionEnvironment = _
  def env: ExecutionEnvironment = _env

  override def beforeAll(): Unit = {
    super.beforeAll()

    val miniClusterResource = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberTaskManagers(1)
        .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
        .build)

    _env = ExecutionEnvironment.getExecutionEnvironment
    _env.setParallelism(4)
    _env.getConfig.disableSysoutLogging()
    _env.getConfig.addDefaultKryoSerializer(classOf[Triple], classOf[TripleSerializer])
    _env.getConfig.addDefaultKryoSerializer(classOf[Node], classOf[DefaultNodeSerializer])
  }

  import org.apache.flink.test.util.MiniClusterWithClientResource
  import org.junit.ClassRule

  private val DEFAULT_PARALLELISM = 4

  @ClassRule val miniClusterResource = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberTaskManagers(1)
      .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
      .build
  )

}
