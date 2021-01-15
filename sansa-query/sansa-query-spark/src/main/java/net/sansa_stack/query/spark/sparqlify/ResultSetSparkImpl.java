package net.sansa_stack.query.spark.sparqlify;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public class ResultSetSparkImpl
    implements JavaResultSetSpark
{
    protected List<Var> resultVars;
    protected JavaRDD<Binding> rdd;

    public ResultSetSparkImpl(List<Var> resultVars, JavaRDD<Binding> rdd) {
        super();
        this.resultVars = resultVars;
        this.rdd = rdd;
    }

    @Override public List<Var> getResultVars() {
        return resultVars;
    }
    @Override public JavaRDD<Binding> getRdd() {
        return rdd;
    }
}
