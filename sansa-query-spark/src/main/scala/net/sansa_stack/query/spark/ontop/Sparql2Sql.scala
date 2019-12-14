package net.sansa_stack.query.spark.ontop

import it.unibz.inf.ontop.injection.OntopSQLOWLAPIConfiguration
import it.unibz.inf.ontop.owlapi.OntopOWLFactory
import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Collectors.joining

object Sparql2Sql {

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

    var conn = reasoner.getConnection()
    var st = conn.createStatement()

    var sqlExecutableQuery = st.getExecutableQuery(sparqlQuery)
    var sqlQuery = sqlExecutableQuery.toString // getSQL();

    sqlQuery
  }

  // def run(sparqlFile: String, r2rmlFile: String, spark: SparkSession) = {
  // }

}

