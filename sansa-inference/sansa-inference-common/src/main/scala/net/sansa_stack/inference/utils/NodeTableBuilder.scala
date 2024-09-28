package net.sansa_stack.inference.utils

import org.apache.jena.graph.NodeFactory
import org.apache.jena.tdb1.base.file.Location
import org.apache.jena.tdb1.setup.DatasetBuilderStd
import tdb.cmdline.CmdTDB
import tdb.xloader.CmdNodeTableBuilder

/**
  * @author Lorenz Buehmann
  */
object NodeTableBuilder {

  def main(args: Array[String]): Unit = {
    CmdTDB.init()
    DatasetBuilderStd.setOptimizerWarningFlag(false)
    new CmdNodeTableBuilder(args: _*).mainRun()

    val location = Location.create("/tmp/node_table")

    // This formats the location correctly.
    // But we're not really interested in it all.
    val dsg = DatasetBuilderStd.create(location)

    // so close indexes and the prefix table.
    val nodeTable = dsg.getTripleTable.getNodeTupleTable.getNodeTable
    val id = nodeTable.getNodeIdForNode(NodeFactory.createURI("http://www.Department0.University6.edu/FullProfessor0"))
    println(id)

    val node = nodeTable.getNodeForNodeId(id)

    println(node)
  }

}
