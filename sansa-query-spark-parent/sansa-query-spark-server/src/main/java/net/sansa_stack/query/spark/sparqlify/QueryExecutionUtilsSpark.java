package net.sansa_stack.query.spark.sparqlify;

import org.aksw.jena_sparql_api.views.RestrictedExpr;
import org.aksw.sparqlify.core.domain.input.SparqlSqlStringRewrite;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Multimap;

import net.sansa_stack.rdf.spark.io.JavaKryoSerializationWrapper;

public class QueryExecutionUtilsSpark {
	public static JavaRDD<Binding> createQueryExecution(SparkSession sparkSession, SparqlSqlStringRewrite rewrite, Query query) {

		Multimap<Var, RestrictedExpr> varDef = rewrite.getVarDefinition().getMap();

		String sqlQueryStr = rewrite.getSqlQueryString();
		Dataset<Row> dataset = sparkSession.sql(sqlQueryStr);

//		System.out.println("SqlQueryStr: " + sqlQueryStr);
//		System.out.println("VarDef: " + rewrite.getVarDefinition());

		SparkRowMapperSparqlify rowMapper = new SparkRowMapperSparqlify(varDef);

		//Function<Row, Binding> fn = x -> rowMapper.apply(x);
		//org.apache.spark.api.java.function.Function<Row, Binding> y = x -> rowMapper.apply(x);

		org.apache.spark.api.java.function.Function<Row, Binding> z = JavaKryoSerializationWrapper.wrap(rowMapper);

		JavaRDD<Binding> result = dataset.javaRDD().map(z);
		return result;
	}
}
