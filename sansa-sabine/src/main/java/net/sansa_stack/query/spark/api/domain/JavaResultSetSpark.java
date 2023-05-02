package net.sansa_stack.query.spark.api.domain;

import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.TableFactory;
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

    /** Load the whole result set into an in-memory Jena table */
    default Table collectToTable() {
        Table result = TableFactory.create(getResultVars());
        getRdd().collect().forEach(result::addBinding);
        return result;
    }
}
