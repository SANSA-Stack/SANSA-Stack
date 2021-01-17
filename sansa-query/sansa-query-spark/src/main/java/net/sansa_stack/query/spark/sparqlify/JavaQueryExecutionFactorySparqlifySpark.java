package net.sansa_stack.query.spark.sparqlify;

import net.sansa_stack.query.spark.api.domain.JavaQueryExecutionFactorySpark;
import org.aksw.jena_sparql_api.core.QueryExecutionFactoryBackQuery;
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter;
import org.apache.jena.query.Query;
import org.apache.spark.sql.SparkSession;

public class JavaQueryExecutionFactorySparqlifySpark
	extends QueryExecutionFactoryBackQuery
		implements JavaQueryExecutionFactorySpark
{
	protected SparkSession sparkSession;
	protected SparqlSqlStringRewriter sparqlSqlRewriter;

	public JavaQueryExecutionFactorySparqlifySpark(SparkSession sparkSession, SparqlSqlStringRewriter sparqlSqlRewriter) {
		super();
		this.sparkSession = sparkSession;
		this.sparqlSqlRewriter = sparqlSqlRewriter;
	}

	@Override
	public JavaQueryExecutionSparkSparqlify createQueryExecution(Query query) {
		JavaQueryExecutionSparkSparqlify result = new JavaQueryExecutionSparkSparqlify(
				query, this, sparkSession, sparqlSqlRewriter);

		return result;
	}

	@Override
	public JavaQueryExecutionSparkSparqlify createQueryExecution(String queryString) {
		return (JavaQueryExecutionSparkSparqlify)super.createQueryExecution(queryString);
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
