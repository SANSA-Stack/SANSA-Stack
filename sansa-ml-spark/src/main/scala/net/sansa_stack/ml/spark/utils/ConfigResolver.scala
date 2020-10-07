package net.sansa_stack.ml.spark.utils

import java.io.{File, InputStreamReader}
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class ConfigResolver(configPath: String) {

  private val HSDF_PREFIX: String = "hdfs"

  /**
   *
   * @param configPath
   * @return
   */
  private def getConfigFromHdfs(configPath: String) = {
    val confFullPath = configPath.split("/")
    val hdfsPart = confFullPath(2)
    var confPath = "/"
    for (i <- 3 to confFullPath.length - 1) {
      if (i == confFullPath.length - 1) {
        confPath += confFullPath(i)
      } else {
        confPath += confFullPath(i) + "/"
      }
    }
    val hdfs: FileSystem = FileSystem.get(new URI("hdfs://" + hdfsPart), new Configuration())
    val reader = new InputStreamReader(hdfs.open(new Path(confPath)))
    ConfigFactory.parseReader(reader)
  }

  /**
   *
   * @param configPath
   * @return
   */
  private def getConfigFromNormalFile(configPath: String) = {
    ConfigFactory.parseFile(new File(configPath))
  }

  /**
   * @return
   */
  def getConfig(): Config = {
    if (configPath == null) {
      throw new IllegalArgumentException("Config file path can not be null")
    }
    if (configPath.startsWith(HSDF_PREFIX)) {
      getConfigFromHdfs(configPath)
    } else {
      getConfigFromNormalFile(configPath)
    }
  }

  def getHdfsPart(configPath: String): String = {
    val confFullPath = configPath.split("/")
    val hdfsPart = confFullPath(2)
    HSDF_PREFIX + "://"+hdfsPart
  }

  def getPathPart(configPath: String): String = {
    val confFullPath = configPath.split("/")
    var confPath = "/"
    for (i <- 3 to confFullPath.length - 1) {
        confPath += confFullPath(i) + "/"
    }
    confPath
  }
}
