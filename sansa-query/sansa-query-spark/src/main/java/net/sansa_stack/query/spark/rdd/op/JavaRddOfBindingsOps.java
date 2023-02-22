package net.sansa_stack.query.spark.rdd.op;

import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import net.sansa_stack.query.spark.api.domain.JavaResultSetSparkImpl;
import net.sansa_stack.query.spark.engine.ExecutionDispatch;
import net.sansa_stack.query.spark.engine.OpExecutor;
import net.sansa_stack.query.spark.engine.OpExecutorImpl;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import org.aksw.jena_sparql_api.algebra.utils.AlgebraUtils;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.optimize.TransformFilterImplicitJoin;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.modify.TemplateLib;
import org.apache.jena.sparql.modify.request.QuadAcc;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class JavaRddOfBindingsOps {

    /** Returns an RDD of a single binding that doesn't bind any variables */
    public static JavaRDD<Binding> unitRdd(JavaSparkContext sparkContext) {
        JavaRDD<Binding> result = sparkContext.parallelize(Arrays.asList(BindingFactory.binding()));
        return result;
    }

    public static JavaRDD<Quad> execSparqlConstruct(JavaRDD<Binding> initialRdd, Query query, Context cxt) {
        Op op = Algebra.compile(query);

        // Set up an execution context
        cxt = cxt == null ? ARQ.getContext().copy() : cxt.copy();
        cxt.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime());
        ExecutionContext execCxt = new ExecutionContext(cxt, null, null, null);
        OpExecutor opExec = new OpExecutorImpl(execCxt);
        ExecutionDispatch executionDispatch = new ExecutionDispatch(opExec);

        // An RDD with a single binding that doesn't bind any variables
        JavaSparkContext sparkContext = JavaSparkContextUtils.fromRdd(initialRdd);

        JavaRDD<Binding> rdd = executionDispatch.exec(op, initialRdd);

        Template template = query.getConstructTemplate();
        List<Quad> quads = template.getQuads();
        return rdd.mapPartitions(it -> TemplateLib.calcQuads(quads, it));
    }

    public static JavaResultSetSpark execSparqlSelect(JavaRDD<? extends Dataset> rddOfDataset,
                                                      Query query, Context cxt) {
        Op op = Algebra.compile(query);

        // op = Transformer.transform(new TransformFilterImplicitJoin(), op);
        // op = AlgebraUtils.createDefaultRewriter().rewrite(op);
        // System.err.println("Algebra: " + op);

        // Set up an execution context
        cxt = cxt == null ? ARQ.getContext().copy() : cxt.copy();
        cxt.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime());
        ExecutionContext execCxt = new ExecutionContext(cxt, null, null, null);
        execCxt.getContext().put(OpExecutorImpl.SYM_RDD_OF_DATASET, rddOfDataset);
        OpExecutor opExec = new OpExecutorImpl(execCxt);
        ExecutionDispatch executionDispatch = new ExecutionDispatch(opExec);

        // An RDD with a single binding that doesn't bind any variables
        JavaSparkContext sparkContext = JavaSparkContextUtils.fromRdd(rddOfDataset);
        JavaRDD<Binding> initialRdd = sparkContext.parallelize(Arrays.asList(BindingFactory.binding()));
        JavaRDD<Binding> rdd = executionDispatch.exec(op, initialRdd);

        List<Var> vars = query.getProjectVars();
        return new JavaResultSetSparkImpl(vars, rdd);
    }
}
