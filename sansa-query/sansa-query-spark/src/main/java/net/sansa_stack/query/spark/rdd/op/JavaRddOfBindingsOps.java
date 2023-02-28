package net.sansa_stack.query.spark.rdd.op;

import com.google.common.base.Preconditions;
import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import net.sansa_stack.query.spark.api.domain.JavaResultSetSparkImpl;
import net.sansa_stack.query.spark.engine.ExecutionDispatch;
import net.sansa_stack.query.spark.engine.OpExecutor;
import net.sansa_stack.query.spark.engine.OpExecutorImpl;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import org.aksw.commons.util.algebra.ExprFilter;
import org.aksw.commons.util.algebra.ExprOps;
import org.aksw.commons.util.algebra.GenericDag;
import org.aksw.jena_sparql_api.algebra.transform.TransformUnionToDisjunction;
import org.aksw.jena_sparql_api.algebra.utils.OpUtils;
import org.aksw.jena_sparql_api.algebra.utils.OpVar;
import org.aksw.jenax.arq.util.syntax.QueryGenerationUtils;
import org.aksw.rml.jena.impl.SparqlX_Rml_Terms;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.optimize.TransformExtendCombine;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Substitute;
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

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class JavaRddOfBindingsOps {
    private static final Logger logger = LoggerFactory.getLogger(JavaRddOfBindingsOps.class);

    /** Returns an RDD of a single binding that doesn't bind any variables */
    public static JavaRDD<Binding> unitRdd(JavaSparkContext sparkContext) {
        JavaRDD<Binding> result = sparkContext.parallelize(Arrays.asList(BindingFactory.binding()));
        return result;
    }

    public static JavaRDD<Quad> execSparqlConstruct(JavaRDD<Binding> initialRdd, List<Query> queries, Context cxt, boolean useDag) {

        Quad quadVars = Quad.create(Var.alloc("__g__"), Var.alloc("__s__"), Var.alloc("__p__"), Var.alloc("__o__"));

        List<Query> lateralConstructQueries = queries.stream()
                .map(query -> QueryGenerationUtils.constructToLateral(query, quadVars, QueryType.CONSTRUCT, false, true))
                .collect(Collectors.toList());

        Op op1 = lateralConstructQueries.stream().map(Algebra::compile).reduce(OpUnion::new).orElse(OpTable.empty());

        // TODO Is it possible that there is a bug that combine extend does not preserve order?
        Op op2 = Transformer.transform(new TransformExtendCombine(), op1);
        // Op op2 = op1;

        // Disjunction as the non-canonical union might be less supported by optimizers
        Op baseOp = Transformer.transform(new TransformUnionToDisjunction(), op2);

        System.out.println(baseOp);
        Op rootOp;
        Map<Var, Op> opDefs;
        if (useDag) {
            GenericDag<Op, Var> dag = buildDag(baseOp);
            rootOp = dag.getRoots().iterator().next();
            opDefs = dag.getVarToExpr();
        } else {
            rootOp = baseOp;
            opDefs = new HashMap<>();
        }

        cxt = cxt == null ? ARQ.getContext().copy() : cxt.copy();
        cxt.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime());
        ExecutionContext execCxt = new ExecutionContext(cxt, null, null, null);
        OpExecutor opExec = new OpExecutorImpl(execCxt, opDefs);
        ExecutionDispatch executionDispatch = new ExecutionDispatch(opExec);

        // An RDD with a single binding that doesn't bind any variables
        JavaSparkContext sparkContext = JavaSparkContextUtils.fromRdd(initialRdd);

        JavaRDD<Binding> rdd = executionDispatch.exec(rootOp, initialRdd);

        JavaRDD<Quad> result = rdd.mapPartitions(it ->
                Iter.iter(it)
                        .map(b -> {
                            Quad q = Substitute.substitute(quadVars, b);
                            return q;
                        })
                        .filter(Quad::isConcrete));
        return result;
    }

    public static boolean defaultBlocker(Op parent, int childIdx, Op child) {
        boolean result = parent instanceof OpService || (parent instanceof OpLateral && childIdx != 0);
        return result;
    }

    public static GenericDag<Op, Var> buildDag(Op rootOp) {
        // Do not descend into the rhs of laterals
        GenericDag<Op, Var> dag = new GenericDag<>(OpUtils.getOpOps(),  new VarAlloc("op")::allocVar, JavaRddOfBindingsOps::defaultBlocker);
        dag.addRoot(rootOp);

        // Insert cache nodes:
        // ?vx      := someOp(vy)
        // becomes
        // ?vx      := cache(vxCache)
        // ?vxCache := someOp(vy)
        Set<Var> cacheCandidates = new HashSet<>();
        for (Map.Entry<Var, Collection<Var>> entry : dag.getChildToParent().asMap().entrySet()) {
            Var childVar = entry.getKey();
            Collection<Var> parentVars = entry.getValue();
            if (parentVars.size() > 1) {
                cacheCandidates.add(childVar);
                // System.out.println("Multiple parents on: " + entry.getKey());
            }
        }
        Set<Op> cacheCandidateOps = cacheCandidates.stream().map(dag::getExpr).collect(Collectors.toSet());


        Map<Op, Float> costs = assessCacheImpact(dag, cacheCandidateOps);
        for (Op candOp : cacheCandidateOps) {
            float cost = costs.get(candOp);
            if (cost < 10000) {
                Var var = dag.getVar(candOp);
                logger.info("Removing low impact cache candidate (cost=" + cost + "): " + var);
                cacheCandidates.remove(var);
            }
        }
        logger.info("Remaining cache nodes: " + cacheCandidates);

        // Remove all ops with a low effectiveness

        for (Var childVar : cacheCandidates) {
            Op childDef = dag.getExpr(childVar);
            Var uncachedVar = Var.alloc(childVar.getName() + "_cached");
            dag.getVarToExpr().remove(childVar);
            dag.getVarToExpr().put(uncachedVar, childDef);
            dag.getVarToExpr().put(childVar, new OpService(NodeFactory.createURI("rdd:cache"), new OpVar(uncachedVar), false));
        }

        dag.collapse();
        logger.info("Roots: " + dag.getRoots());
        for (Map.Entry<Var, Op> e : dag.getVarToExpr().entrySet()) {
            // logger.info(e.toString());
            System.err.println(e.toString());
        }

        // if (true) { throw new RuntimeException(); }

        return dag;
    }


    /**
     * Remove cache operations closer to the root that do not bring sufficient benefit over cache operations lower in the tree.
     * Uses a simple cost heuristic:
     * - rml sources have high cost
     * - joins have high cost
     * - rest is cheap (e.g. extends)
     */
    public static Map<Op, Float> assessCacheImpact(GenericDag<Op, Var> dag, Set<Op> cacheCandidateOp) {
        Map<Op, Float> costs = new HashMap<>();
        Consumer<Op> computeCost = op -> {
            float cost = assessCostContribution(op, costs, cacheCandidateOp);
            costs.put(op, cost);
        };
        for (Op root : dag.getRoots()) {
            GenericDag.depthFirstTraverse(dag, null, 0, root, JavaRddOfBindingsOps::defaultBlocker, computeCost);
        }
        return costs;
    }

    public static boolean isCacheOp(Op op) { return OpUtils.isServiceWithIri(op, "rdd:cache"); }
    public static boolean isRmlSourceOp(Op op) { return OpUtils.isServiceWithIri(op, SparqlX_Rml_Terms.RML_SOURCE_SERVICE_IRI); }


    /**
     * A cacheable node itself has a (typically high) cost - however, a cacheable node in the role of a sub-node has cost 0.
     *
     * @param op
     * @param costs
     * @param cacheCandidates
     * @return
     */
    public static float assessCostContribution(Op op, Map<Op, Float> costs, Set<Op> cacheCandidates) {
        float result;
        // if (isCacheOp(op)) {
        if (isRmlSourceOp(op)) {
            result = 1_000_000;
        } else if (op instanceof OpJoin || op instanceof OpDisjunction) {
            result = 1_000_000;
        } else {
            List<Op> subOps = OpUtils.getSubOps(op);
            float maxCost = 0;
            for (Op subOp : subOps) {
                float cost;
                if (cacheCandidates.contains(subOp)) {
                    cost = 0;
                } else {
                    Float tmp = costs.get(subOp);
                    if (tmp == null) {
                        // If traversal into a sub op was blocked then count it as 0
                        cost = 0;
                        // throw new RuntimeException("Should not happen: No cost entry for " + subOp);
                    } else {
                        cost = tmp;
                    }
                }
                if (cost > maxCost) {
                    maxCost = cost;
                }
            }
            // Certainly cheap ops: extend, project, union, disjunction
            // Possibly expensive ops: filter (if selective)
            result = maxCost + 1;
        }
        return result;
    }

    //    public static <E, V> void pruneSuperfluousCacheOps(GenericDag<E, V> dag, ExprFilter<E> isBlocked) {
//        for (E root : dag.getRoots()) {
//            pruneSuperfluousCacheOps(dag, null, 0, root, costs, isBlocked);
//        }
//    }

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
