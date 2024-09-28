package net.sansa_stack.query.spark.sparqlify;

import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import net.sansa_stack.query.spark.api.domain.JavaResultSetSparkImpl;
import net.sansa_stack.query.spark.api.impl.JavaQueryExecutionSparkBase;
import org.aksw.jenax.dataaccess.sparql.factory.execution.query.QueryExecutionFactory;
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite;
import org.aksw.sparqlify.core.interfaces.SparqlSqlStringRewriter;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class JavaQueryExecutionSparkSparqlify
		extends JavaQueryExecutionSparkBase {

	protected SparqlSqlStringRewriter sparqlSqlRewriter;

	public JavaQueryExecutionSparkSparqlify(Query query, QueryExecutionFactory subFactory, SparkSession sparkSession,
											SparqlSqlStringRewriter sparqlSqlRewriter) {
		super(query, subFactory, sparkSession);
		this.sparqlSqlRewriter = sparqlSqlRewriter;
	}

	@Override
	public JavaResultSetSpark execSelectSparkJava() {
		SparqlSqlStringRewrite rewrite = sparqlSqlRewriter.rewrite(query);
		List<Var> resultVars = rewrite.getProjectionOrder();

		JavaRDD<Binding> rdd = QueryExecutionUtilsSpark.createQueryExecution(sparkSession, rewrite, query);

		JavaResultSetSpark result = new JavaResultSetSparkImpl(resultVars, rdd);
		return result;
	}
}
