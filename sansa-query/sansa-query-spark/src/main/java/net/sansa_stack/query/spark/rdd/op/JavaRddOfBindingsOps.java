package net.sansa_stack.query.spark.rdd.op;

import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import net.sansa_stack.query.spark.api.domain.JavaResultSetSparkImpl;
import net.sansa_stack.query.spark.engine.ExecutionDispatch;
import net.sansa_stack.query.spark.engine.OpExecutor;
import net.sansa_stack.query.spark.engine.OpExecutorImpl;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import org.aksw.commons.util.algebra.GenericDag;
import org.aksw.jena_sparql_api.algebra.utils.OpUtils;
import org.aksw.jena_sparql_api.algebra.utils.OpVar;
import org.aksw.jenax.arq.util.syntax.QueryGenerationUtils;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpDisjunction;
import org.apache.jena.sparql.algebra.op.OpLateral;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarAlloc;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.modify.TemplateLib;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JavaRddOfBindingsOps {
    private static final Logger logger = LoggerFactory.getLogger(JavaRddOfBindingsOps.class);

    /** Returns an RDD of a single binding that doesn't bind any variables */
    public static JavaRDD<Binding> unitRdd(JavaSparkContext sparkContext) {
        JavaRDD<Binding> result = sparkContext.parallelize(Arrays.asList(BindingFactory.binding()));
        return result;
    }

    public static JavaRDD<Quad> execSparqlConstruct(JavaRDD<Binding> initialRdd, List<Query> queries, Context cxt) {
        Quad quadVars = Quad.create(Var.alloc("__g__"), Var.alloc("__s__"), Var.alloc("__p__"), Var.alloc("__o__"));

        List<Query> lateralConstructQueries = queries.stream()
                .map(query -> QueryGenerationUtils.constructToLateral(query, quadVars, false))
                .collect(Collectors.toList());

        GenericDag<Op, Var> dag = buildDag(lateralConstructQueries);
        Op rootOp = dag.getRoots().iterator().next();


        cxt = cxt == null ? ARQ.getContext().copy() : cxt.copy();
        cxt.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime());
        ExecutionContext execCxt = new ExecutionContext(cxt, null, null, null);
        OpExecutor opExec = new OpExecutorImpl(execCxt, dag.getVarToExpr());
        ExecutionDispatch executionDispatch = new ExecutionDispatch(opExec);

        // An RDD with a single binding that doesn't bind any variables
        JavaSparkContext sparkContext = JavaSparkContextUtils.fromRdd(initialRdd);

        JavaRDD<Binding> rdd = executionDispatch.exec(rootOp, initialRdd);

        List<Quad> quads = Arrays.asList(quadVars);
        JavaRDD<Quad> result = rdd.mapPartitions(it -> TemplateLib.calcQuads(quads, it));
        return result;
    }


    public static GenericDag<Op, Var> buildDag(Collection<Query> queries) {
        List<Op> ops = queries.stream().map(Algebra::compile).collect(Collectors.toList());
        OpDisjunction union = OpDisjunction.create();
        ops.forEach(union::add);
        // Do not descend into the rhs of laterals
        GenericDag<Op, Var> dag = new GenericDag<>(OpUtils.getOpOps(),  new VarAlloc("op")::allocVar, (p, i, c) -> c instanceof OpService || (p instanceof OpLateral && i != 0));
        dag.addRoot(union);

        // Insert cache nodes
        // vx := someOp(vy) becomes
        // vx := cache(vxCache)
        // vxCache := someOp(vy)
        for (Map.Entry<Var, Collection<Var>> entry : dag.getChildToParent().asMap().entrySet()) {
            if (entry.getValue().size() > 1) {
                // System.out.println("Multiple parents on: " + entry.getKey());
                Var v = entry.getKey();
                Op def = dag.getVarToExpr().get(v);
                Var uncachedVar = Var.alloc(v.getName() + "_cached");
                dag.getVarToExpr().remove(v);
                dag.getVarToExpr().put(uncachedVar, def);
                dag.getVarToExpr().put(v, new OpService(NodeFactory.createURI("rdd:cache"), new OpVar(uncachedVar), false));
            }
        }

        dag.collapse();
        logger.info("Roots: " + dag.getRoots());
        for (Map.Entry<Var, Op> e : dag.getVarToExpr().entrySet()) {
            logger.info(e.toString());
        }
        // throw new RuntimeException("test");
        return dag;
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

    public static JavaResultSetSpark execSparqlSelect(JavaRDD<? extends Dataset> rddOfDataset, Query query, Context cxt) {
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
