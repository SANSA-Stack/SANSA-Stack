package org.aksw.s2rdf.example

import org.aksw.s2rdf.dataset_creator.runDriver
import org.apache.commons.io.FileUtils
import java.io.File
import org.aksw.s2rdf.executor.query.QueryExecutor
import org.aksw.s2rdf.executor.query.runDriverQuery
import org.aksw.s2rdf.dataset_creator.Settings

object MainS2RdfExample {
  def main(args : Array[String]) : Unit = {
    val srcFolder = new File("src/main/resources/")
    println(srcFolder.getAbsolutePath)
    val tgtFolder = new File("target/example/")

    FileUtils.deleteDirectory(tgtFolder)
    tgtFolder.mkdirs()

    val inFile = new File(srcFolder, "s2rdf-example-data.nt")
    FileUtils.copyFile(inFile, new File(tgtFolder, inFile.getName))




    List("VP", "SS", "SO", "OS").foreach(
        joinType => runDriver.main(Array(tgtFolder.getAbsolutePath + "/", inFile.getName, joinType, "1")))

    Settings.sparkContext.stop()

    //if(true) { return }

    val queryFile = new File(tgtFolder, "query.sparql")
    FileUtils.write(queryFile, "SELECT ?s { ?s <http://ex.org/b> ?o . ?s <http://ex.org/x> ?o2}")

    val statsFolder = tgtFolder

    val queryPlanFile = new File(tgtFolder, "query.sparql.sql")
    queryTranslator.run.Main.main(Array(
        "-f", tgtFolder.getAbsolutePath,
        "-i", queryFile.getAbsolutePath,
        "-o", queryPlanFile.getAbsolutePath,
        "-sd", statsFolder.getAbsolutePath,
        "-so",
        "-ss",
        "-os"))

    runDriverQuery.main(Array(tgtFolder.getAbsolutePath, queryPlanFile.getAbsolutePath))


    //println("yay")
  }
}