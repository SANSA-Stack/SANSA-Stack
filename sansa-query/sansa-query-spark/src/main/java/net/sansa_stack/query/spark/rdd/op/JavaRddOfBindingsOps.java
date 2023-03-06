package net.sansa_stack.query.spark.rdd.op;

import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import net.sansa_stack.query.spark.api.domain.JavaResultSetSparkImpl;
import net.sansa_stack.query.spark.engine.ExecutionDispatch;
import net.sansa_stack.query.spark.engine.OpExecutor;
import net.sansa_stack.query.spark.engine.OpExecutorImpl;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import org.aksw.commons.util.algebra.GenericDag;
import org.aksw.jena_sparql_api.algebra.transform.TransformUnionToDisjunction;
import org.aksw.jena_sparql_api.algebra.utils.OpUtils;
import org.aksw.jena_sparql_api.algebra.utils.OpVar;
import org.aksw.jenax.arq.util.syntax.QueryGenerationUtils;
import org.aksw.jenax.arq.util.syntax.VarExprListUtils;
import org.aksw.jenax.sparql.algebra.transform2.Evaluator;
import org.aksw.jenax.sparql.algebra.transform2.OpCost;
import org.aksw.rml.jena.impl.SparqlX_Rml_Terms;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryType;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.optimize.TransformExtendCombine;
import org.apache.jena.sparql.core.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.modify.TemplateLib;
import org.apache.jena.sparql.syntax.Template;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class JavaRddOfBindingsOps {
    private static final Logger logger = LoggerFactory.getLogger(JavaRddOfBindingsOps.class);

    /** Returns an RDD of a single binding that doesn't bind any variables */
    public static JavaRDD<Binding> unitRdd(JavaSparkContext sparkContext) {
        JavaRDD<Binding> result = sparkContext.parallelize(Arrays.asList(BindingFactory.binding()));
        return result;
    }

    public static JavaRDD<Quad> execSparqlConstruct(JavaRDD<Binding> initialRdd, List<Query> queries, Supplier<ExecutionContext> execCxtSupplier, boolean useDag) {

        Quad tmpConstructQuad = null;

        // If there is just a single query that projects a single quad then don't apply construct-to-lateral transformation
        //  because it may separate Order+Distinct operations and thus miss optimization opportunity
        List<Query> effectiveQueries = null;
        if (queries.size() == 1) {
            Query query = queries.iterator().next();
            List<Quad> constructQuads = query.getConstructTemplate().getQuads();
            if (constructQuads.size() == 1) {
                tmpConstructQuad = constructQuads.iterator().next();
                effectiveQueries = Collections.singletonList(query);
            }
        }

        Quad constructQuad = tmpConstructQuad != null
            ? tmpConstructQuad
            : Quad.create(Var.alloc("__g__"), Var.alloc("__s__"), Var.alloc("__p__"), Var.alloc("__o__"));

        if (effectiveQueries == null) {
            // TODO Variables of the query may clash with the tmpConstructQuad
            effectiveQueries = queries.stream()
                    .map(query -> QueryGenerationUtils.constructToLateral(query, constructQuad, QueryType.CONSTRUCT, false, true))
                    .collect(Collectors.toList());
        }

        Op op1 = effectiveQueries.stream().map(Algebra::compile).reduce(OpUnion::new).orElse(OpTable.empty());
        Op op2 = Transformer.transform(new TransformExtendCombine(), op1);

        // Disjunction as the non-canonical union might be less supported by optimizers
        Op op3 = Transformer.transform(new TransformUnionToDisjunction(), op2);

        OpCost opCost = Evaluator.evaluate(new JoinOrderOptimizer(Path::of), op3);
        Op baseOp = opCost.getOp();

        if (logger.isInfoEnabled()) {
            logger.info("Algebra: " + baseOp);
        }
        // System.out.println(baseOp);
        Op rootOp;
        Map<Var, Op> opDefs;
        if (useDag) {
            GenericDag<Op, Var> dag = CacheOptimizer.buildDag(baseOp);
            rootOp = dag.getRoots().iterator().next();
            opDefs = dag.getVarToExpr();
        } else {
            rootOp = baseOp;
            opDefs = new HashMap<>();
        }

//        SerializableSupplier execCxtSupplier = () -> {
//            Context cxt = cxt == null ? ARQ.getContext().copy() : cxt.copy();
//            cxt.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime());
//            ExecutionContext execCxt = new ExecutionContext(cxt, null, null, null);
//            return execCxt;
//        };

        OpExecutor opExec = new OpExecutorImpl(execCxtSupplier, opDefs);
        ExecutionDispatch executionDispatch = new ExecutionDispatch(opExec);

        // An RDD with a single binding that doesn't bind any variables
        JavaSparkContext sparkContext = JavaSparkContextUtils.fromRdd(initialRdd);

        JavaRDD<Binding> rdd = executionDispatch.exec(rootOp, initialRdd);

        JavaRDD<Quad> result = rdd.mapPartitions(it ->
                Iter.iter(it)
                        .map(b -> {
                            Quad q = Substitute.substitute(constructQuad, b);
                            return q;
                        })
                        .filter(Quad::isConcrete));
        return result;
    }


    public static JavaRDD<Binding> filter(JavaRDD<Binding> rdd, ExprList exprs, Supplier<ExecutionContext> execCxtSupplier) {
        Broadcast<ExprList> broadcast = JavaSparkContextUtils.fromRdd(rdd).broadcast(exprs);
        return rdd.mapPartitions(it -> {
                ExprList el = broadcast.value();
                ExecutionContext execCxt = execCxtSupplier.get();
                return Iter.iter(it)
                        .filter(b -> el.isSatisfied(b, execCxt));
        });
    }

    public static JavaRDD<Binding> extend(JavaRDD<Binding> rdd, VarExprList varExprList, Supplier<ExecutionContext> execCxtSupplier) {
        // TODO We should pass an execCxt
        JavaSparkContext sc = JavaSparkContextUtils.fromRdd(rdd);
        Broadcast<VarExprList> velBc = sc.broadcast(varExprList);
        return rdd.mapPartitions(it -> {
                ExecutionContext execCxt = execCxtSupplier.get();
                // v execCxt = ExecutionContextUtils.createExecCxtEmptyDsg()
                VarExprList vel = velBc.value();
                return Iter.iter(it).map(b -> {
                        Binding r = VarExprListUtils.eval(vel, b, execCxt);
                        return r;
                });
        });
    }

    //    public static <E, V> void pruneSuperfluousCacheOps(GenericDag<E, V> dag, ExprFilter<E> isBlocked) {
//        for (E root : dag.getRoots()) {
//            pruneSuperfluousCacheOps(dag, null, 0, root, costs, isBlocked);
//        }
//    }

    public static JavaRDD<Quad> execSparqlConstruct(JavaRDD<Binding> initialRdd, Query query, Supplier<ExecutionContext> execCxtSupplier) {
        Op op = Algebra.compile(query);

        // Set up an execution context
//        cxt = cxt == null ? ARQ.getContext().copy() : cxt.copy();
//        cxt.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime());
//        ExecutionContext execCxt = new ExecutionContext(cxt, null, null, null);
        OpExecutor opExec = new OpExecutorImpl(execCxtSupplier);
        ExecutionDispatch executionDispatch = new ExecutionDispatch(opExec);

        // An RDD with a single binding that doesn't bind any variables
        JavaSparkContext sparkContext = JavaSparkContextUtils.fromRdd(initialRdd);

        JavaRDD<Binding> rdd = executionDispatch.exec(op, initialRdd);

        Template template = query.getConstructTemplate();
        List<Quad> quads = template.getQuads();
        return rdd.mapPartitions(it -> TemplateLib.calcQuads(quads, it));
    }

    public static JavaResultSetSpark execSparqlSelect(JavaRDD<? extends Dataset> rddOfDataset, Query query, Supplier<ExecutionContext> execCxtSupplier) {
        Op op = Algebra.compile(query);

        // op = Transformer.transform(new TransformFilterImplicitJoin(), op);
        // op = AlgebraUtils.createDefaultRewriter().rewrite(op);
        // System.err.println("Algebra: " + op);

        // Set up an execution context
        OpExecutor opExec = new OpExecutorImpl(execCxtSupplier);

        ExecutionDispatch executionDispatch = new ExecutionDispatch(opExec);

        // An RDD with a single binding that doesn't bind any variables
        JavaSparkContext sparkContext = JavaSparkContextUtils.fromRdd(rddOfDataset);
        JavaRDD<Binding> initialRdd = sparkContext.parallelize(Arrays.asList(BindingFactory.binding()));
        JavaRDD<Binding> rdd = executionDispatch.exec(op, initialRdd);

        List<Var> vars = query.getProjectVars();
        return new JavaResultSetSparkImpl(vars, rdd);
    }
}
