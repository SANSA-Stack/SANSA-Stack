package org.aksw.s2rdf.example

import org.aksw.s2rdf.dataset_creator.runDriver
import org.apache.commons.io.FileUtils
import java.io.File

object MainS2RdfExample {
  def main(args : Array[String]) = {
    // TODO copy test file to target
    val workingFolder = new File("target/example")
    workingFolder.delete()
    val inFile = "s2rdf-example-data.nt"

    workingFolder.mkdirs()

    FileUtils.copyFile(new File("src/main/resources", inFile), new File(workingFolder, inFile))

    List("ss", "so", "os").foreach(joinType => runDriver.main(Array(workingFolder.getAbsolutePath, inFile, joinType, "1")))


    //println("yay")
  }
}