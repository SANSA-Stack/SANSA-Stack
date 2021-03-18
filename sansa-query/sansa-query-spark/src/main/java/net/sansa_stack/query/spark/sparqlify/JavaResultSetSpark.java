package net.sansa_stack.query.spark.sparqlify;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * An interface to represent a SPARQL result set by bundling
 * a (Java)RDD of Bindings together with a list of result variables.
 *
 * Use {@code getRdd().rdd()} to obtain the scala RDD.
 *
 */
public interface JavaResultSetSpark {
    List<Var> getResultVars();
    JavaRDD<Binding> getRdd();
}
