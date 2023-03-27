package net.sansa_stack.query.spark.api.domain;

import org.aksw.jenax.arq.connection.core.QueryExecutionFactory;
import org.apache.jena.query.Query;

public interface JavaQueryExecutionFactorySpark
    extends QueryExecutionFactory
{
    @Override
    JavaQueryExecutionSpark createQueryExecution(Query query);

    @Override
    JavaQueryExecutionSpark createQueryExecution(String queryString);
}
