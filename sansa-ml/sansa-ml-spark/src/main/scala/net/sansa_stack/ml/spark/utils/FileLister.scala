package net.sansa_stack.ml.spark.utils

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

object FileLister {

  private val HSDF_PREFIX: String = "hdfs"

  def getListOfFiles(dir: String): List[String] = {
    if (dir == null) {
      throw new IllegalArgumentException("file path can not be null")
    }
    if (dir.startsWith(HSDF_PREFIX)) {
      getListOfHDFSFiles(dir)
    } else {
      getListOfNormalFiles(dir)
    }
  }

  def getListOfNormalFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile).map(_.getPath.toString).toList
  }

  def getListOfHDFSFiles(dir: String): List[String] = {
    val fileSystem: FileSystem = {
      val conf = new Configuration()
      conf.set("fs.defaultFS", dir)
      FileSystem.get(conf)
    }
    val files: RemoteIterator[LocatedFileStatus] = fileSystem.listFiles(new Path(dir), false)
    var result: List[String] = List()

    while (files.hasNext) {
      result = result :+ files.next().getPath.toString
    }
    result
  }
}
