package net.sansa_stack.rdf.spark.sparqlify;

import java.util.Iterator;
import java.util.List;

import org.aksw.jena_sparql_api.core.QueryExecutionBaseSelect;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.core.ResultSetCloseable;
import org.aksw.jena_sparql_api.utils.ResultSetUtils;
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite;
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class QueryExecutionSparqlifySpark
	extends QueryExecutionBaseSelect
{
	public QueryExecutionSparqlifySpark(
			Query query,
			QueryExecutionFactory subFactory,
			SparkSession sparkSession,
			SparqlSqlStringRewriter sparqlSqlRewriter
			) {
		super(query, subFactory);
		this.sparkSession = sparkSession;
		this.sparqlSqlRewriter = sparqlSqlRewriter;
	}

	protected SparkSession sparkSession;
	protected SparqlSqlStringRewriter sparqlSqlRewriter;

    @Override
    protected ResultSetCloseable executeCoreSelect(Query query) {
		SparqlSqlStringRewrite rewrite = sparqlSqlRewriter.rewrite(query);
		List<Var> resultVars = rewrite.getProjectionOrder();

    	JavaRDD<Binding> rdd = QueryExecutionUtilsSpark.createQueryExecution(sparkSession, rewrite, query);
    	Iterator<Binding> it = rdd.toLocalIterator();

    	ResultSet tmp = ResultSetUtils.create2(resultVars, it);
    	ResultSetCloseable result = new ResultSetCloseable(tmp);
    	return result;
    }

	@Override
	protected QueryExecution executeCoreSelectX(Query query) {
		throw new UnsupportedOperationException();
	}

}
