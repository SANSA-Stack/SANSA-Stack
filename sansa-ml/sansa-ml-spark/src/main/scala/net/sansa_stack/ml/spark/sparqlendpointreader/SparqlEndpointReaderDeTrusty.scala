package net.sansa_stack.ml.spark.sparqlendpointreader

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.jena.atlas.json.JsonObject
import scalaj.http.{Http, HttpRequest}

import java.util.Date

class SparqlEndpointReaderDeTrusty {
  var host: String = ""
  var sparql: String = ""
  var port: String = ""
  var sparqlQuery: String = ""
  var outputFileName: String = ""
  var hdfsHost: String = ""

  def setOutputFileName(
      outputFileName: String
  ): SparqlEndpointReaderDeTrusty = {
    if (outputFileName == null) {
      throw new Exception("outputFileName can not be null")
    }

    this.outputFileName = outputFileName
    this
  }

  def setHdfsHost(hdfsHost: String): SparqlEndpointReaderDeTrusty = {
    if (hdfsHost == null) {
      throw new Exception("hdfsHost can not be null")
    }

    this.hdfsHost = hdfsHost
    this
  }

  def setHost(host: String): SparqlEndpointReaderDeTrusty = {
    if (host == null) {
      throw new Exception("URL can not be null")
    }

    this.host = host
    this
  }

  def setSparql(sparql: String): SparqlEndpointReaderDeTrusty = {
    if (sparql == null) {
      throw new Exception("URL can not be null")
    }
    if (sparql.trim.isEmpty) {
      throw new Exception("Sparql can not be empty")
    }

    this.sparql = sparql
    this
  }

  def setPort(port: String): SparqlEndpointReaderDeTrusty = {
    if (port == null) {
      throw new Exception("PORT can not be null")
    }
    if (port.trim.isEmpty) {
      throw new Exception("PORT can not be empty")
    }
    if (!port.forall(Character.isDigit)) {
      throw new Exception("PORT is not valid. Your url is " + port)
    }
    this.port = port
    this
  }

  def setSparqlQuery(sparqlQuery: String): SparqlEndpointReaderDeTrusty = {
    if (sparqlQuery == null) {
      throw new Exception("SparqlQuery can not be null")
    }
    if (sparqlQuery.trim.isEmpty) {
      throw new Exception("SparqlQuery can not be empty")
    }
    this.sparqlQuery = sparqlQuery
    this
  }

  def getDataAddressOnHDFS(): String = {
    var url: String = ""
    if (port == null || port.isEmpty) {
      url = host + "/" + sparql
    } else {
      url = host + ":" + port + "/" + sparql
    }

    println(new Date().toString() + url)
    println(new Date().toString() + " Start")

    val result: HttpRequest = Http(url)
      .postData(
        "query=" + sparqlQuery
      )
      .timeout(100000000, 100000000)

    import org.apache.jena.atlas.json.JSON
    val json: JsonObject = JSON.parse(result.asString.body)
    println(new Date().toString() + " End")
    val bindings = json
      .getObj("results")
      .getArray("bindings")

    val res: StringBuilder = new StringBuilder()
    bindings.forEach(binding => {

      val objectValue =
        binding
          .asInstanceOf[JsonObject]
          .get("o")
          .getAsObject
          .get("value")
          .getAsString
      val objectType =
        binding
          .asInstanceOf[JsonObject]
          .get("o")
          .getAsObject
          .get("type")
          .getAsString
      val subjectValue =
        binding
          .asInstanceOf[JsonObject]
          .get("s")
          .getAsObject
          .get("value")
          .getAsString
      val subjectType =
        binding
          .asInstanceOf[JsonObject]
          .get("s")
          .getAsObject
          .get("type")
          .getAsString
      val predicateValue =
        binding
          .asInstanceOf[JsonObject]
          .get("p")
          .getAsObject
          .get("value")
          .getAsString
      val predicateType =
        binding
          .asInstanceOf[JsonObject]
          .get("p")
          .getAsObject
          .get("type")
          .getAsString

      res
        .append("<")
        .append(subjectValue.value())
        .append(">")
        .append(" ")
        .append("<")
        .append(predicateValue.value())
        .append(">")
        .append(" ")

      if (objectType.value().equals("literal")) {
        res.append(objectValue)
      } else if (objectType.value().equals("uri")) {
        res.append("<").append(objectValue.value()).append(">")
      }
      res.append(".\n")
    })

    val outputFullPath = hdfsHost + "user/root/" + outputFileName
    val path = new Path(outputFullPath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsHost)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(res.toString().getBytes)
    fs.close()
    outputFullPath
  }

}
