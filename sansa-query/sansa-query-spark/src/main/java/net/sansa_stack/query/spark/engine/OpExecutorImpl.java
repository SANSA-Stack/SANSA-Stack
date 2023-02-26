package net.sansa_stack.query.spark.engine;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.maps.internal.ratelimiter.LongMath;
import net.sansa_stack.query.spark.rdd.op.JavaRddOfBindingsOps;
import net.sansa_stack.query.spark.rdd.op.RddOfBindingsOps;
import net.sansa_stack.rdf.spark.rdd.op.RddOfDatasetsOps;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import org.aksw.jena_sparql_api.algebra.utils.OpUtils;
import org.aksw.jena_sparql_api.algebra.utils.OpVar;
import org.aksw.jenax.arq.util.exec.ExecutionContextUtils;
import org.aksw.jenax.arq.util.syntax.QueryUtils;
import org.aksw.rml.jena.impl.RmlLib;
import org.aksw.rml.jena.impl.SparqlX_Rml_Terms;
import org.aksw.rml.model.LogicalSource;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.*;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.sse.WriterSSE;
import org.apache.jena.sparql.util.Symbol;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;


public class OpExecutorImpl
        implements OpExecutor {
    public static final Symbol SYM_RDD_OF_DATASET = Symbol.create("urn:rddOfDataset");

    protected ExecutionContext execCxt;

    /** Algebra expressions may use OpVar instances which then get resolved against this map. */
    protected Map<Var, Op> varToOp;
    protected ExecutionDispatch dispatcher = new ExecutionDispatch(this);
    protected int level = 0;

    /** FIXME ExecCxt is not serializable; we an only use a serializable lambda that produces a context in the workers */
    public OpExecutorImpl(ExecutionContext execCxt) {
        this(execCxt, new HashMap<>());
    }

    public OpExecutorImpl(ExecutionContext execCxt, Map<Var, Op> varToOp) {
        super();
        this.varToOp = varToOp;
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
    public JavaRDD<Binding> execute(OpVar op, JavaRDD<Binding> rdd) {
        Var var = op.getVar();
        Op resolvedOp = varToOp.get(var);
        Preconditions.checkState(resolvedOp != null, "OpVar " + var + " has no defining Op");
        return execToRdd(resolvedOp, rdd).toJavaRDD();
    }

    @Override
    public JavaRDD<Binding> execute(OpProject op, JavaRDD<Binding> rdd) {
        return RddOfBindingsOps.project(execToRdd(op.getSubOp(), rdd), op.getVars()).toJavaRDD();
    }
    // RddOfBindingOps.project(rdd, op.getVars)

    @Override
    public JavaRDD<Binding> execute(OpDisjunction op, JavaRDD<Binding> rdd) {
        List<Op> ops = op.getElements();
        return executeUnion(rdd, ops);
    }

    public JavaRDD<Binding> executeUnion(JavaRDD<Binding> rdd, List<Op> ops) {
        JavaRDD<Binding>[] rdds = ops.stream().map(o -> execToRdd(o, rdd).toJavaRDD()).collect(Collectors.toList()).toArray(new JavaRDD[0]);
        JavaSparkContext sc = JavaSparkContextUtils.fromRdd(rdd);
        JavaRDD<Binding> result = sc.union(rdds);
        return result;
    }

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
            if (("rdd:cache").equals(serviceUri)) {
                JavaRDD<Binding> base = execToRdd(op.getSubOp(), rdd).toJavaRDD();
                result = base.persist(StorageLevel.MEMORY_AND_DISK());
                success = true;
            } else if ("rdd:perPartition".equals(serviceUri)) {
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
            } else if (SparqlX_Rml_Terms.RML_SOURCE_SERVICE_IRI.equals(serviceUri)) {
                JavaSparkContext sc = JavaSparkContextUtils.fromRdd(rdd);
                LogicalSource logicalSource = RmlLib.getLogicalSource(op);
                Preconditions.checkArgument(logicalSource != null, "No logical source detected in " + op);
                result = RmlSourcesSpark.processSource(sc, logicalSource, null, execCxt);
                success = true;
            }
        }

        if (!success) {
            throw new IllegalArgumentException("Execution with service " + serviceNode + " is not supported");
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
        return executeUnion(rdd, Arrays.asList(op.getLeft(), op.getRight()));
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
        JavaRDD<Binding> base = execToRdd(op.getSubOp(), rdd).toJavaRDD();

        long start = op.getStart();
        long length = op.getLength();

        long begin = start == Query.NOLIMIT ? 0 : start;
        long end = length == Query.NOLIMIT ? Long.MAX_VALUE : LongMath.saturatedAdd(begin, length);

        // Do not apply zip with index if the range is [0, max]
        JavaRDD<Binding> result = (begin == 0 && end == Long.MAX_VALUE)
                ? base
                : base.zipWithIndex().filter(t -> t._2 >= begin && t._2 < end).map(t -> t._1);
        return result;
    }


    @Override
    public JavaRDD<Binding> execute(OpJoin op, JavaRDD<Binding> input) {
        Op lhsOp = op.getLeft();
        Op rhsOp = op.getRight();

        Set<Var> lhsVars = OpVars.visibleVars(lhsOp);
        Set<Var> rhsVars = OpVars.visibleVars(rhsOp);

        Set<Var> joinVars = new LinkedHashSet<>(Sets.intersection(lhsVars, rhsVars));

        JavaRDD<Binding> lhsRdd = execToRdd(lhsOp, input).toJavaRDD();
        JavaRDD<Binding> rhsRdd = execToRdd(rhsOp, root(input)).toJavaRDD();

        JavaPairRDD<Long, Binding> lhsPairRdd = hashForJoin(lhsRdd, joinVars);
        JavaPairRDD<Long, Binding> rhsPairRdd = hashForJoin(rhsRdd, joinVars);

        JavaRDD<Binding> result = lhsPairRdd.join(rhsPairRdd)
                .map(t -> t._2)
                .filter(t -> Algebra.compatible(t._1, t._2))
                .map(t -> Binding.builder(t._1).addAll(t._2).build());

        return result;
    }

    @Override
    public JavaRDD<Binding> execute(OpLateral op, JavaRDD<Binding> rdd) {
        JavaRDD<Binding> base = execToRdd(op.getLeft(), rdd).toJavaRDD();
        JavaRDD<Binding> result;
        boolean isPatternFree = OpUtils.isPatternFree(op.getRight());
        if (isPatternFree) {
            // Just use flat map without going throw the whole spark machinery
            String rightSse = op.getRight().toString(); // Produces parsable SSE!
            result = base.mapPartitions(it -> {
                Op rightOp = SSE.parseOp(rightSse);
                ExecutionContext execCxt = ExecutionContextUtils.createExecCxtEmptyDsg();
                return Iter.iter(it).flatMap(b -> QC.execute(rightOp, b, execCxt));
            });
        } else {
            throw new UnsupportedOperationException("Lateral joins for non-pattern-free ops not yet implemented");
        }
        return result;
    }

    public static JavaPairRDD<Long, Binding> hashForJoin(JavaRDD<Binding> rdd, Set<Var> joinVars) {
        return rdd.mapPartitionsToPair(itBindings ->
                Iter.map(itBindings, binding -> {
                    Long hash = JoinLib.hash(joinVars, binding);
                    return new Tuple2<>(hash, binding);
                })
        );

    }

    /** Create an RDD with a single empty binding */
    protected JavaRDD<Binding> root(JavaRDD<Binding> prototype) {
        JavaSparkContext sc = JavaSparkContextUtils.fromRdd(prototype);
        return JavaRddOfBindingsOps.unitRdd(sc);
    }

}
