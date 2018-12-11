package net.sansa_stack.query.spark.sparql2sql

import it.unibz.inf.ontop.answering.reformulation.impl.SQLExecutableQuery
import it.unibz.inf.ontop.injection.OntopSQLOWLAPIConfiguration
import it.unibz.inf.ontop.owlapi.OntopOWLFactory
import it.unibz.inf.ontop.owlapi.OntopOWLReasoner
import it.unibz.inf.ontop.owlapi.connection.OntopOWLConnection
import it.unibz.inf.ontop.owlapi.connection.OntopOWLStatement
import it.unibz.inf.ontop.owlapi.resultset.GraphOWLResultSet
import it.unibz.inf.ontop.owlapi.resultset.OWLBinding
import it.unibz.inf.ontop.owlapi.resultset.OWLBindingSet
import it.unibz.inf.ontop.owlapi.resultset.TupleOWLResultSet
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Collectors.joining
import org.semanticweb.owlapi.io.ToStringRenderer
import org.semanticweb.owlapi.model.OWLAxiom
import org.semanticweb.owlapi.model.OWLException
import org.semanticweb.owlapi.model.OWLObject

object Sparql2Sql {

    def obtainSQL(sparqlFile: String, r2rmlFile: String, owlFile: String, propertyFile: String): String = {
        var factory = OntopOWLFactory.defaultFactory();

        var config = OntopSQLOWLAPIConfiguration.defaultBuilder()
                .r2rmlMappingFile(r2rmlFile)
                .ontologyFile(owlFile)
                .propertyFile(propertyFile)
                .enableTestMode()
                .build();

        var reasoner = factory.createReasoner(config);

        /*
         * Prepare the data connection for querying.
         */
        var sparqlQuery = Files.lines(Paths.get(sparqlFile)).collect(joining("\n"));

        var conn = reasoner.getConnection();
        var st = conn.createStatement();

        var sqlExecutableQuery = st.getExecutableQuery(sparqlQuery);
        var sqlQuery = sqlExecutableQuery.toString // getSQL();

        sqlQuery
    }

    // def run(sparqlFile: String, r2rmlFile: String, spark: SparkSession) = {
    // }

}

