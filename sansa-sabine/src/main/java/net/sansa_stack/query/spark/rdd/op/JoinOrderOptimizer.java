package net.sansa_stack.query.spark.rdd.op;

import org.aksw.jena_sparql_api.algebra.utils.OpServiceUtils;
import org.aksw.jenax.model.csvw.domain.api.Table;
import org.aksw.jenax.sparql.algebra.transform2.OpCost;
import org.aksw.jenax.sparql.algebra.transform2.OpCostEvaluation;
import org.aksw.rml.jena.impl.RmlLib;
import org.aksw.rmltk.model.backbone.rml.ILogicalSource;
import org.aksw.rmlx.model.RmlXTerms;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

/**
 * Analyze both sides of a join for whether rml sources are used.
 * If it turns out that one side uses a very small source than "wrap" the join with a parent
 * OpService("rdd:broadcastJoin", OpJoin(lhs, rhs)) node.
 *
 */
public class JoinOrderOptimizer
    extends OpCostEvaluation
{
    protected float broadcastJoinCostThreshold;
    protected Function<String, Path> sourceResolver;

    public JoinOrderOptimizer(Function<String, Path> sourceResolver) {
        this(sourceResolver, 50_000_000);
    }

    public JoinOrderOptimizer(Function<String, Path> sourceResolver, float broadcastJoinCostThreshold) {
        this.sourceResolver = sourceResolver;
        this.broadcastJoinCostThreshold = broadcastJoinCostThreshold;
    }

    /**
     * Check whether on either side of the join is a small data source.
     * If one is found then place it on the right-hand-side (if it isn't there already)
     */
    @Override
    public OpCost eval(OpJoin op, OpCost left, OpCost right) {
        OpCost result;
        float lhsCost = left.getCost();
        float rhsCost = right.getCost();

        // Ensure that lhs has higher cost - swap arguments if needed
        if (rhsCost > lhsCost) {
            { OpCost tmp = left; left = right; right = tmp; }
            { float tmp = lhsCost; lhsCost = rhsCost; rhsCost = tmp; }
        }

        // broadcast join threshold
        if (rhsCost < broadcastJoinCostThreshold) {
            result = new OpCost(new OpService(NodeFactory.createURI("rdd:broadcastJoin"),
                    OpJoin.create(left.getOp(), right.getOp()), false), lhsCost * rhsCost);
        } else {
            result = super.eval(op, left, right);
        }
        return result;
    }

    @Override
    public OpCost eval(OpService op, OpCost subCost) {
        OpCost result = null;
        if (RmlXTerms.RML_SOURCE_SERVICE_IRI.equals(OpServiceUtils.getIriOrNull(op))) {
            // Get the logical source and use its size as the cost
            // XXX Probably we shouldn't use the byte size directly but use some rough estimate
            // to get the number of records - but for the purpose of identifying some
            // broadcast joins it might be sufficient
            ILogicalSource logicalSource = RmlLib.getLogicalSource(op);
            String source = resolveSource(logicalSource.getSource());

            Path path = sourceResolver.apply(source);
            long sourceSize = 0;
            try {
                sourceSize = Files.size(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            result = new OpCost(op, sourceSize);

        }
        if (result == null) {
            result = super.eval(op, subCost);
        }
        return result;
    }

    private static String resolveSource(RDFNode source) {
        // TODO add dcat support
        if (source.isLiteral()) {
            return source.asLiteral().getLexicalForm();
        } else {
            Table table = source.as(Table.class);
            String url = table.getUrl();
            return url != null ? url : source.asNode().getURI();
        }
    }
}
