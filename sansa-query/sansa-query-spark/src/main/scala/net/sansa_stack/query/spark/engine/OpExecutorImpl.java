package net.sansa_stack.query.spark.engine;

import net.sansa_stack.query.spark.rdd.op.RddOfBindingsOps;
import net.sansa_stack.rdf.spark.rdd.op.RddOfDatasetsOps;
import org.aksw.jenax.arq.util.syntax.QueryUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.Symbol;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;


public class OpExecutorImpl
        implements OpExecutor {
    public static final Symbol SYM_RDD_OF_DATASET = Symbol.create("urn:rddOfDataset");

    protected ExecutionContext execCxt;
    protected ExecutionDispatch dispatcher = new ExecutionDispatch(this);
    protected int level = 0;

    public OpExecutorImpl(ExecutionContext execCxt) {
        super();
        this.execCxt = execCxt;
    }

    public JavaRDD<Binding> exec(Op op, JavaRDD<Binding> input) {
        level += 1;
        JavaRDD<Binding> result = dispatcher.exec(op, input);
        level -= 1;
        return result;
    }

    public RDD<Binding> execToRdd(Op op, JavaRDD<Binding> input) {
        return exec(op, input).rdd();
    }

    @Override
    public JavaRDD<Binding> execute(OpProject op, JavaRDD<Binding> rdd) {
        return RddOfBindingsOps.project(execToRdd(op.getSubOp(), rdd), op.getVars()).toJavaRDD();
    }
    // RddOfBindingOps.project(rdd, op.getVars)

    @Override
    public JavaRDD<Binding> execute(OpGroup op, JavaRDD<Binding> rdd) {
        return RddOfBindingsOps.group(execToRdd(op.getSubOp(), rdd), op.getGroupVars(), op.getAggregators()).toJavaRDD();
//    RddOfBindingOps.group(rdd, op.getGroupVars, op.getAggregators)
    }

    @Override
    public JavaRDD<Binding> execute(OpService op, JavaRDD<Binding> rdd) {
        JavaRDD<Binding> result = null;

        Node serviceNode = op.getService();
        var success = false;

        if (serviceNode.isURI()) {
            String serviceUri = serviceNode.getURI();

            // TODO Add some registry
            // TODO Consider deprecation and/or removalof rdd:perPartition because of the scalability issues when loading them into RAM
            if ("rdd:perPartition".equals(serviceUri)) {
                // Get the RDD[Dataset] from the execution context
                JavaRDD<Dataset> rddOfDataset = execCxt.getContext().get(SYM_RDD_OF_DATASET);

                if (rddOfDataset == null) {
                    throw new RuntimeException("No rddOfDataset in execution context - cannot delegate to " + serviceUri);
                }

                Query query = op.getServiceElement() != null
                        ? QueryUtils.elementToQuery(op.getServiceElement())
                        : OpAsQuery.asQuery(op.getSubOp());

                result = RddOfDatasetsOps.selectWithSparqlPerPartition(rddOfDataset.rdd(), query).toJavaRDD();

                success = true;
            } else if ("rdd:perGraph".equals(serviceUri)) {
                // Get the RDD[Dataset] from the execution context
                JavaRDD<Dataset> rddOfDataset = execCxt.getContext().get(SYM_RDD_OF_DATASET);

                if (rddOfDataset == null) {
                    throw new RuntimeException("No rddOfDataset in execution context - cannot delegate to " + serviceUri);
                }

                Query query = op.getServiceElement() != null
                        ? QueryUtils.elementToQuery(op.getServiceElement())
                        : OpAsQuery.asQuery(op.getSubOp());

                result = RddOfDatasetsOps.flatMapWithSparqlSelect(rddOfDataset.rdd(), query).toJavaRDD();

                success = true;
            }
        }

        if (!success) {
            throw new IllegalArgumentException("Execution with service " + serviceNode + " is not supportd");
        }

        return result;
    }

    @Override
    public JavaRDD<Binding> execute(OpOrder op, JavaRDD<Binding> rdd) {
        return RddOfBindingsOps.order(execToRdd(op.getSubOp(), rdd), op.getConditions()).toJavaRDD();
    }

    @Override
    public JavaRDD<Binding> execute(OpExtend op, JavaRDD<Binding> rdd) {
        return RddOfBindingsOps.extend(execToRdd(op.getSubOp(), rdd), op.getVarExprList()).toJavaRDD();
    }

    @Override
    public JavaRDD<Binding> execute(OpUnion op, JavaRDD<Binding> rdd) {
        // TODO This method should get the (spark-based) executor
        // and pass all union members to it
        throw new UnsupportedOperationException();
    }

    @Override
    public JavaRDD<Binding> execute(OpDistinct op, JavaRDD<Binding> rdd) {
        return execToRdd(op.getSubOp(), rdd).distinct().toJavaRDD();
    }

    @Override
    public JavaRDD<Binding> execute(OpReduced op, JavaRDD<Binding> rdd) {
        return execToRdd(op.getSubOp(), rdd).distinct().toJavaRDD();
    }

    @Override
    public JavaRDD<Binding> execute(OpFilter op, JavaRDD<Binding> rdd) {
        return RddOfBindingsOps.filter(execToRdd(op.getSubOp(), rdd), op.getExprs()).toJavaRDD();
        // RddOfBindingOps.filter(rdd, op.getExprs)
    }

    //  @Override public  JavaRDD<Binding> execute(OpSlice op, JavaRDD<Binding> rdd): JavaRDD<Binding> =
    @Override
    public JavaRDD<Binding> execute(OpSlice op, JavaRDD<Binding> rdd) {
        throw new UnsupportedOperationException();
    }
}
