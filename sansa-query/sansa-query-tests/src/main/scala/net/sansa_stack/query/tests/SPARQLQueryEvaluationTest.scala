package net.sansa_stack.query.tests

/**
 * A single test case for SPARQL query evaluation.
 * @param uri the URI of the test case resource
 * @param name the name of the test case
 * @param description an (optional) description of the test case
 * @param queryFile the path to the file containing the query to evaluate
 * @param dataFile the path to the file containing the data on which the query will be evaluated
 * @param resultsFile the path to the file containg the result of the query evaluation, i.e. the target solution
 *
 * @author Lorenz Buehmann
 */
case class SPARQLQueryEvaluationTest(uri: String,
                                     name: String,
                                     description: String,
                                     queryFile: String,
                                     dataFile: String,
                                     resultsFile: Option[String])
