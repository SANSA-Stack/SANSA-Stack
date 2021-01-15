package net.sansa_stack.query.spark.api.domain;

import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;

public interface JavaQueryExecutionFactorySpark
    extends QueryExecutionFactory
{
    @Override
    JavaQueryExecutionSpark createQueryExecution(Query query);

    @Override
    JavaQueryExecutionSpark createQueryExecution(String queryString);
}
