package org.aksw.s2rdf.example

import org.aksw.s2rdf.dataset_creator.runDriver
import org.apache.commons.io.FileUtils
import java.io.File
import org.aksw.s2rdf.executor.query.QueryExecutor

object MainS2RdfExample {
  def main(args : Array[String]) = {
    // TODO copy test file to target
    val workingFolder = new File("target/example/")
    FileUtils.deleteDirectory(workingFolder)
    val inFile = "s2rdf-example-data.nt"

    val queryPlanFile = new File(workingFolder, "queryPlan.dat")

    workingFolder.mkdirs()

    FileUtils.copyFile(new File("src/main/resources/", inFile), new File(workingFolder, inFile))

    List("VP", "ss", "so", "os").foreach(
        joinType => runDriver.main(Array(workingFolder.getAbsolutePath + "/", inFile, joinType, "1")))


    val queryFile = new File(workingFolder, "query.txt")
    FileUtils.write(queryFile, "SELECT * { ?s ?p ?o }")

    val statsFolder = workingFolder


    queryTranslator.run.Main.main(Array(
        "-f", workingFolder.getAbsolutePath,
        "-i", queryFile.getName,
        "-o", queryPlanFile.getName,
        "-sd", statsFolder.getAbsolutePath,
        "-so",
        "-ss",
        "-os"))

    runDriver.main(Array(workingFolder.getAbsolutePath, queryPlanFile.getName))


    //println("yay")
  }
}