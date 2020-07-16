package net.sansa_stack.query.spark.ontop

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors.joining

import it.unibz.inf.ontop.injection.OntopSQLOWLAPIConfiguration
import it.unibz.inf.ontop.owlapi.OntopOWLFactory

@deprecated("will be removed, use OntopSPARQLEngine", "Sparql2Sql 0.8.0")
object Sparql2Sql {

  val baseDir = new File("/tmp/ontop-spark")
  baseDir.mkdirs()

  def obtainSQL(sparqlFile: String, r2rmlFile: String, owlFile: String, propertyFile: String): String = {
    var factory = OntopOWLFactory.defaultFactory()

    var config = OntopSQLOWLAPIConfiguration.defaultBuilder()
      .r2rmlMappingFile(r2rmlFile)
      .ontologyFile(owlFile)
      .propertyFile(propertyFile)
      .enableTestMode()
      .build()

    var reasoner = factory.createReasoner(config)

    /*
     * Prepare the data connection for querying.
     */
    var sparqlQuery = Files.lines(Paths.get(sparqlFile)).collect(joining("\n"))

    var conn = reasoner.getConnection
    var st = conn.createStatement()

    var sqlExecutableQuery = st.getExecutableQuery(sparqlQuery)
    var sqlQuery = sqlExecutableQuery.toString // getSQL();

    sqlQuery
  }

}

