package net.sansa_stack.query.spark.sparqlify;

import com.google.common.collect.Multimap;
import net.sansa_stack.rdf.spark.utils.kryo.io.JavaKryoSerializationWrapper;
import org.aksw.jena_sparql_api.views.RestrictedExpr;
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExecutionUtilsSpark {
	private static final Logger logger = LoggerFactory.getLogger(QueryExecutionUtilsSpark.class);

	public static JavaRDD<Binding> createQueryExecution(SparkSession sparkSession, SparqlSqlStringRewrite rewrite, Query query) {

		Multimap<Var, RestrictedExpr> varDef = rewrite.getVarDefinition().getMap();

		String sqlQueryStr = rewrite.getSqlQueryString();

		// FIXME HACK to get rid of incorrect double precision types in CASTs; needs fix in sparqlify
		sqlQueryStr = sqlQueryStr.replaceAll("AS double precision\\)", "AS double)");

		// FIXME HACK to get rid of '... ON (true)' join conditions
		sqlQueryStr = sqlQueryStr.replaceAll("ON \\(TRUE\\)", "");


		Dataset<Row> dataset = sparkSession.sql(sqlQueryStr);

		logger.debug("SqlQueryStr: " + sqlQueryStr);
//		System.out.println("VarDef: " + rewrite.getVarDefinition());

		SparkRowMapperSparqlify rowMapper = new SparkRowMapperSparqlify(varDef);

		//Function<Row, Binding> fn = x -> rowMapper.apply(x);
		//org.apache.spark.api.java.function.Function<Row, Binding> y = x -> rowMapper.apply(x);

		org.apache.spark.api.java.function.Function<Row, Binding> z = JavaKryoSerializationWrapper.wrap(rowMapper);

		JavaRDD<Binding> result = dataset.javaRDD().map(z);
		return result;
	}
}
