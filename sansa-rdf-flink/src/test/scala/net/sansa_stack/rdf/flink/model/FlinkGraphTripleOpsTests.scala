package net.sansa_stack.rdf.flink.model

import net.sansa_stack.rdf.common.kryo.jena.JenaKryoSerializers._
import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.graph.Node
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class FlinkGraphTripleOpsTests extends FunSuite {

  val env = ExecutionEnvironment.getExecutionEnvironment
  test("constructing the Graph from DataSet should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    import org.apache.flink.api.scala._

    val nodeInfo: TypeInformation[Node] = createTypeInformation[Node]
    nodeInfo.createSerializer(env.getConfig)
    env.getConfig.enableForceKryo()

    val triples = env.rdf(lang)(path)

    env.getConfig.addDefaultKryoSerializer(classOf[Node], classOf[NodeSerializer])
    env.registerTypeWithKryoSerializer(classOf[Node], classOf[NodeSerializer])
    env.registerTypeWithKryoSerializer(classOf[org.apache.jena.graph.Node], classOf[NodeSerializer])
    env.registerTypeWithKryoSerializer(classOf[Array[org.apache.jena.graph.Node]], classOf[NodeSerializer])

    // val graph = triples.asGraph()

    // val size = graph.size()

    assert(triples.count() == 106)

  }
}
