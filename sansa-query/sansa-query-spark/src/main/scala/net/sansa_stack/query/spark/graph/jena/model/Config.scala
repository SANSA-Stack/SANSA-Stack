package net.sansa_stack.query.spark.graph.jena.model

import org.apache.jena.riot.Lang

/**
  * Configuration parameters for spark session.
  */
object Config {

  private var master = ""

  def getMaster: String = { master }

  def setMaster(master: String): this.type = {
    this.master = master
    this
  }

  private var appName = ""

  def getAppName: String = { appName }

  def setAppName(appName: String): this.type = {
    this.appName = appName
    this
  }

  private var inputGraphFile = ""

  def getInputGraphFile: String = { inputGraphFile }

  def setInputGraphFile(path: String): this.type = {
    this.inputGraphFile = path
    this
  }

  private var inputQueryFile = ""

  def getInputQueryFile: String = { inputQueryFile }

  def setInputQueryFile(path: String): this.type = {
    this.inputQueryFile = path
    this
  }

  private var lang: Lang = _

  def getLang: Lang = { lang }

  def setLang(lang: Lang): this.type = {
    this.lang = lang
    this
  }
}
