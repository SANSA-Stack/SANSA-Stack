package org.aksw.s2rdf.example

import org.aksw.s2rdf.dataset_creator.runDriver
import org.apache.commons.io.FileUtils
import java.io.File
import org.aksw.s2rdf.executor.query.QueryExecutor

object MainS2RdfExample {
  def main(args : Array[String]) = {
    // TODO copy test file to target
    val srcFolder = new File("src/main/resources/");
    val tgtFolder = new File("target/example/")

    FileUtils.deleteDirectory(tgtFolder)
    tgtFolder.mkdirs()

    val inFile = new File(srcFolder, "s2rdf-example-data.nt")
    FileUtils.copyFile(inFile, new File(tgtFolder, inFile.getName))

    val queryPlanFile = new File(tgtFolder, "queryPlan.dat")


    List("VP", "ss", "so", "os").foreach(
        joinType => runDriver.main(Array(tgtFolder.getAbsolutePath + "/", inFile.getName, joinType, "1")))


    val queryFile = new File(tgtFolder, "query.txt")
    FileUtils.write(queryFile, "SELECT * { ?s ?p ?o }")

    val statsFolder = tgtFolder


    queryTranslator.run.Main.main(Array(
        "-f", tgtFolder.getAbsolutePath,
        "-i", queryFile.getName,
        "-o", queryPlanFile.getName,
        "-sd", statsFolder.getName,
        "-so",
        "-ss",
        "-os"))

    runDriver.main(Array(tgtFolder.getAbsolutePath, queryPlanFile.getName))


    //println("yay")
  }
}