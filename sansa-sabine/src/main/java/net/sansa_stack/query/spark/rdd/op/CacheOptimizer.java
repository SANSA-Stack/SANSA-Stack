package net.sansa_stack.query.spark.rdd.op;

import org.aksw.commons.util.algebra.GenericDag;
import org.aksw.jena_sparql_api.algebra.utils.OpUtils;
import org.aksw.jena_sparql_api.algebra.utils.OpVar;
import org.aksw.rmlx.model.NorseRmlTerms;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpDisjunction;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpLateral;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarAlloc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CacheOptimizer {

    private static final Logger logger = LoggerFactory.getLogger(JavaRddOfBindingsOps.class);

    public static boolean defaultBlocker(Op parent, int childIdx, Op child) {
        boolean result = parent instanceof OpService || (parent instanceof OpLateral && childIdx != 0);
        return result;
    }

    public static GenericDag<Op, Var> buildDag(Op rootOp) {
        // Do not descend into the rhs of laterals
        GenericDag<Op, Var> dag = new GenericDag<>(OpUtils.getOpOps(),  new VarAlloc("op")::allocVar, CacheOptimizer::defaultBlocker);
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
            GenericDag.depthFirstTraverse(dag, null, 0, root, CacheOptimizer::defaultBlocker, computeCost);
        }
        return costs;
    }

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

    public static boolean isCacheOp(Op op) { return OpUtils.isServiceWithIri(op, "rdd:cache"); }
    public static boolean isRmlSourceOp(Op op) { return OpUtils.isServiceWithIri(op, NorseRmlTerms.RML_SOURCE_SERVICE_IRI); }

}
