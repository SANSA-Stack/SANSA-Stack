package net.sansa_stack.query.spark.api.domain;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

public interface JavaQueryExecutionSpark
    extends QueryExecution
{
    JavaResultSetSpark execSelectSparkJava();
    JavaRDD<Triple> execConstructSparkJava();
    JavaRDD<Quad> execConstructQuadsSparkJava();
}
