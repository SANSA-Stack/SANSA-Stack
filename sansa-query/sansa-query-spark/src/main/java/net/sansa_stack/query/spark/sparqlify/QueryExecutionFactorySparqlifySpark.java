package net.sansa_stack.query.spark.sparqlify;

import org.aksw.jena_sparql_api.core.QueryExecutionFactoryBackQuery;
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter;
import org.apache.jena.query.Query;
import org.apache.spark.sql.SparkSession;

public class QueryExecutionFactorySparqlifySpark
	extends QueryExecutionFactoryBackQuery
{
	protected SparkSession sparkSession;
	protected SparqlSqlStringRewriter sparqlSqlRewriter;

	public QueryExecutionFactorySparqlifySpark(SparkSession sparkSession, SparqlSqlStringRewriter sparqlSqlRewriter) {
		super();
		this.sparkSession = sparkSession;
		this.sparqlSqlRewriter = sparqlSqlRewriter;
	}

	@Override
	public QueryExecutionSparqlifySpark createQueryExecution(Query query) {
		QueryExecutionSparqlifySpark result = new QueryExecutionSparqlifySpark(
				query, this, sparkSession, sparqlSqlRewriter);

		return result;
	}

	@Override
	public String getId() {
		return "spark";
	}

	@Override
	public String getState() {
		return sparkSession.toString();
	}
}
