package net.sansa_stack.spark.rdd.op.rdf;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.commons.rx.function.RxFunction;
import org.aksw.commons.util.function.TriConsumer;
import org.aksw.commons.util.stream.StreamFunction;
import org.aksw.jena_sparql_api.rx.query_flow.QueryFlowOps;
import org.aksw.jena_sparql_api.rx.query_flow.QueryStreamOps;
import org.aksw.jenax.arq.util.exec.ExecutionContextUtils;
import org.aksw.jenax.arq.util.quad.QuadPatternUtils;
import org.aksw.jenax.arq.util.syntax.QueryUtils;
import org.aksw.jenax.arq.util.update.UpdateUtils;
import org.aksw.jenax.stmt.core.SparqlStmt;
import org.aksw.jenax.stmt.util.SparqlStmtUtils;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.optimize.TransformExtendCombine;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.modify.TemplateLib;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.system.Txn;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;

import net.sansa_stack.spark.rdd.op.rx.JavaRddRxOps;
import net.sansa_stack.spark.util.JavaSparkContextUtils;

public class JavaRddOfBindingsOps {

    /** The special ?ROWNUM variable supported by tarql */
    public static final Var ROWNUM = Var.alloc("ROWNUM");

    /** 'Static' means that the dataset is fixed */
//    public static Function<Binding, Stream<Triple>> compileStaticMapperTriple(Query query) {
//        DatasetGraph ds = DatasetGraphFactory.empty();
//        Context cxt = ARQ.getContext().copy();
//        ExecutionContext execCxt = new ExecutionContext(cxt, ds.getDefaultGraph(), ds, QC.getFactory(cxt));
//    }

    /**
     * Compile a construct query into a function that can efficiently produce triples/quads
     * from a given binding.
     * The query is internally stored in algebra form to allow for fast execution. 
     */
    public static <T> BiFunction<Binding, ExecutionContext, Stream<T>> compileNodeTupleMapper(
    		Query query,
    		Function<Template, Function<Binding, Stream<T>>> templateMapperFactory) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");
		Template template = query.getConstructTemplate();
        Op op = Algebra.compile(query);
        Op finalOp = tarqlOptimize(op);

        // Context cxt = ARQ.getContext().copy();
        Function<Binding, Stream<T>> templateMapper = templateMapperFactory.apply(template);
        
        return (binding, execCxt) -> {
            QueryIterator r = QC.execute(finalOp, binding, execCxt);
            return Streams.stream(r).onClose(r::close).flatMap(templateMapper);
        };    	
    }

    public static <I, O> Function<I, O> bindToEmptyDataset(BiFunction<I, ExecutionContext, O> fn) {
    	return bindToDataset(fn, DatasetGraphFactory.empty());
    }
    
    /** Bind a node tuple mapper to a fixed execution context. */
    public static <I, O> Function<I, O> bindToDataset(BiFunction<I, ExecutionContext, O> fn, DatasetGraph ds) {
        Context cxt = ARQ.getContext().copy();
        OpExecutorFactory opExecutorFactory = QC.getFactory(cxt);
        ExecutionContext execCxt = new ExecutionContext(cxt, ds.getDefaultGraph(), ds, opExecutorFactory);
        return in -> fn.apply(in, execCxt);
    }

    public static Function<Binding, Stream<Triple>> templateMapperTriples(Template template) {
    	List<Triple> triples = template.getTriples();
        return binding -> Iter.asStream(TemplateLib.calcTriples(triples, Collections.singleton(binding).iterator()));
    }

    public static Function<Binding, Stream<Quad>> templateMapperQuads(Template template) {
    	List<Quad> quads = template.getQuads();
        return binding -> Iter.asStream(TemplateLib.calcQuads(quads, Collections.singleton(binding).iterator()));
    }

    public static Function<Binding, Stream<Quad>> compileTarqlMapper(List<SparqlStmt> stmts, boolean constructMode) {
    	// If all queries are pattern-free construct queries we can use the fastest processing path
    	// If a query uses patterns then we need to set up a dataset against which those patterns can be executed
    
    	Function<Binding, DatasetGraph> base = compileTarqlMapperGeneral(stmts, constructMode);
    	return binding -> Iter.asStream(base.apply(binding).find());
    }

    public static Function<Binding, DatasetGraph> compileTarqlMapperGeneral(Collection<SparqlStmt> stmts, boolean constructMode) {
    	List<TriConsumer<Binding, ExecutionContext, DatasetGraph>> actions = stmts.stream().map(stmt -> {
    		TriConsumer<Binding, ExecutionContext, DatasetGraph> r;
    		if (stmt.isQuery()) {
    			Query query = stmt.getQuery();
				if (query.isConstructType()) {
	    			BiFunction<Binding, ExecutionContext, Stream<Quad>> ntm = compileNodeTupleMapper(stmt.getQuery(), JavaRddOfBindingsOps::templateMapperQuads);
	    			// TODO We are updating the dataset while our query is running; we may have to store the quads in a temp collection first
	    			r = (b, execCxt, outDs) -> ntm.apply(b, execCxt).forEach(outDs::add);
				} else  if (query.isSelectType()) {
					// TODO Print out result set
					throw new UnsupportedOperationException();
//					r = (b, execCxt, outDs) -> {
//						QueryExecDataset.newBuilder().dataset(execCxt.getDataset())
//						.substitution(b).s
//						System.err.println(ResultSetFormatter.asText(qe.execSelect()));

				} else if (query.isAskType()) {
					// TODO Print out ask result
					throw new UnsupportedOperationException();
					// System.err.println(qe.execAsk());
				} else {
					throw new IllegalStateException("Unknown query type: " + query);
				}
    		} else {
    			r = (b, execCxt, outDs) -> UpdateExec
    					.dataset(execCxt.getDataset())
    					.substitution(b)
    					.update(stmt.getUpdateRequest())
    					.execute();
    		}    		
    		return r;
    	})
    	.collect(Collectors.toList());
    	
    	
    	return binding -> {
	    	DatasetGraph result = DatasetGraphFactory.createGeneral();	  
	    	DatasetGraph inputDs = constructMode ? DatasetGraphFactory.createGeneral() : result;
	    	ExecutionContext execCxt = ExecutionContextUtils.createExecCxt(inputDs);
    		Txn.executeWrite(inputDs, () -> {
    			for (TriConsumer<Binding, ExecutionContext, DatasetGraph> action : actions) {
    				action.accept(binding, execCxt, result);
    			}
    		});
    		// System.out.println("Returned model size: " + Iter.count(result.asDatasetGraph().find()));
	    	return result;
    	};
    }

    public static boolean mayProduceQuads(Collection<SparqlStmt> stmts) {
    	return stmts.stream().anyMatch(JavaRddOfBindingsOps::mayProduceQuads);
    }
    
    public static boolean mayProduceQuads(SparqlStmt stmt) {
    	boolean result;
    	if (!stmt.isParsed()) {
    		result = true;
    	} else if (stmt.isQuery()) {
    		Query query = stmt.getQuery();
    		result = !(query.isConstructType() && query.isConstructQuad());    		
    	} else {
    		result = true; // Updates may affect quads - we don't know
    	}
    	return result;
    }
    
    /**
     * Turns each row into a dataset based on SPARQL update statements.
     * Construct queries and select queries are print out to STDERR.
     * Use {@link UpdateUtils.constructToInsert} to convert construct queries.
     */
    public static JavaRDD<Dataset> tarqlDatasets(JavaRDD<Binding> rdd, Collection<SparqlStmt> stmts, boolean constructMode) {
        boolean usesRowNum = mentionesRowNum(stmts);
        rdd = usesRowNum ? enrichRddWithRowNum(rdd) : rdd;
        return JavaRddOps.mapPartitions(rdd, upstream -> {
            Function<Binding, DatasetGraph> mapper = compileTarqlMapperGeneral(stmts, constructMode);        	
        	return upstream.map(mapper).map(DatasetFactory::wrap);
        });
    }
    
    public static JavaRDD<Triple> tarqlTriples(JavaRDD<Binding> rdd, Collection<SparqlStmt> stmts, boolean constructMode) {
    	JavaRDD<Triple> result;

    	// If we are in constructMode and there are no update statements then use the fast track
    	// Also, if there is 
    	// TODO We can also use fast track if the queries are pattern free so that they cannot refer to the
    	// output of a prior query
    	boolean allQueries = stmts.stream().allMatch(SparqlStmt::isQuery);
    	boolean canUseFastTrack =
    			(constructMode && allQueries) || (allQueries && stmts.size() < 2);
    			
    	boolean usesRowNum = mentionesRowNum(stmts);
        rdd = usesRowNum ? enrichRddWithRowNum(rdd) : rdd;    	
    	
    	if (canUseFastTrack) {
    		result = JavaRddOps.mapPartitions(rdd, bindings -> {
    			// The SparqlStmt-to-Query conversion has to be done here because the latter is not serializable
        		List<Query> queries = stmts.stream().map(SparqlStmt::getQuery).collect(Collectors.toList());
        		StreamFunction<Binding, Triple> mapper = tripleMapper(queries);
        		return mapper.apply(bindings);    		
    		});
    	} else {
    		result = JavaRddOfDatasetsOps.flatMapToTriples(tarqlDatasets(rdd, stmts, constructMode));
    	}
    	return result;
    }

    public static JavaRDD<Quad> tarqlQuads(JavaRDD<Binding> rdd, Collection<SparqlStmt> stmts, boolean constructMode) {
    	JavaRDD<Quad> result;

    	// If we are in constructMode and there are no update statements then use the fast track
    	// Also, if there is 
    	// TODO We can also use fast track if the queries are pattern free so that they cannot refer to the
    	// output of a prior query
    	boolean allQueries = stmts.stream().allMatch(SparqlStmt::isQuery);
    	boolean canUseFastTrack =
    			(constructMode && allQueries) || (allQueries && stmts.size() < 2);
    			
    	boolean usesRowNum = mentionesRowNum(stmts);
        rdd = usesRowNum ? enrichRddWithRowNum(rdd) : rdd;    	
    	
    	if (canUseFastTrack) {
    		result = JavaRddOps.mapPartitions(rdd, bindings -> {
    			// The SparqlStmt-to-Query conversion has to be done here because the latter is not serializable
        		List<Query> queries = stmts.stream().map(SparqlStmt::getQuery).collect(Collectors.toList());
        		StreamFunction<Binding, Quad> mapper = quadMapper(queries);
        		return mapper.apply(bindings);    		
    		});
    	} else {
    		result = JavaRddOfDatasetsOps.flatMapToQuads(tarqlDatasets(rdd, stmts, constructMode));
    	}
    	return result;
    }

    
    // LEGACY CODE BELOW - needs cleanup!
    
    
    public static StreamFunction<Binding, Triple> tripleMapperStream(Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

		Template template = query.getConstructTemplate();
        Op op = Algebra.compile(query);
        Op finalOp = tarqlOptimize(op);

        return StreamFunction.<Binding>identity()
                .andThen(JavaRddOfBindingsOps.createMapperBindings(finalOp))
                .andThenFlatMap(JavaRddOfBindingsOps.createMapperTriples(template)::apply);
    }

    public static StreamFunction<Binding, Quad> quadMapper(Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

		Template template = query.getConstructTemplate();
        Op op = Algebra.compile(query);
        Op finalOp = tarqlOptimize(op);

        return StreamFunction.identity(Binding.class)
                .andThen(QueryStreamOps.createMapperBindings(finalOp))
                .andThenFlatMap(QueryStreamOps.createMapperQuads(template)::apply);
    }
    
    public static StreamFunction<Binding, Triple> tripleMapper(Collection<Query> queries) {
    	List<Function<Binding, Stream<Triple>>> mappers = queries.stream()
    			.map(q -> bindToEmptyDataset(compileNodeTupleMapper(q, JavaRddOfBindingsOps::templateMapperTriples)))
    			.collect(Collectors.toList());
    	
    	// For every input binding apply all mappers
    	return upstream -> upstream.flatMap(binding ->
    		mappers.stream().flatMap(mapper -> mapper.apply(binding)));
    }

    public static StreamFunction<Binding, Quad> quadMapper(Collection<Query> queries) {
    	List<StreamFunction<Binding, Quad>> mappers = queries.stream()
    			.map(q -> quadMapper(q))
    			.collect(Collectors.toList());
    	
    	// For every input binding apply all mappers
    	return upstream -> upstream.flatMap(binding ->
    		mappers.stream().flatMap(mapper -> mapper.apply(Stream.of(binding))));
    }

    /**
     * Use an RDD of bindings as initial bindings for a construct query in order to yield triples.
     * This is conceptually the same approach as done by the tool 'tarql', hence the name.
     */
//    public static JavaRDD<Triple> tarqlTriples(JavaRDD<Binding> rdd, Collection<Query> queries) {
//        // On xps 17: processing times of stream vs rx on pdl data yields 3:15 vs 3:30min; so stream is faster ~ Claus
//        return tarqlTriplesStream(rdd, queries);
//    }

//    public static JavaRDD<Triple> tarqlTriplesOld(JavaRDD<Binding> rdd, Query query) {
//    	return tarqlTriples(rdd, Collections.singletonList(query));
//    }
//    
//    public static JavaRDD<Triple> tarqlTriplesOld(JavaRDD<Binding> rdd, Collection<Query> queries) {
//    	// Called for validation
//    	tripleMapper(queries);
//        
//    	boolean usesRowNum = mentionesRowNumQuery(queries);
//        rdd = usesRowNum ? enrichRddWithRowNum(rdd) : rdd;
//
//        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
//        Broadcast<Collection<Query>> queryBc = cxt.broadcast(queries);
//        return JavaRddOps.mapPartitions(rdd, bindingIt -> {
//            Collection<Query> qs = queryBc.getValue();
//            return tripleMapper(qs).apply(bindingIt);
//        });
//    }

//    public static JavaRDD<Quad> tarqlQuads(JavaRDD<Binding> rdd, Query query) {
//    	return tarqlQuads(rdd, Collections.singletonList(query));
//    }
//
//    public static JavaRDD<Quad> tarqlQuads(JavaRDD<Binding> rdd, Collection<Query> queries) {
//    	// Called for validation
//    	quadMapper(queries);
//        
//    	boolean usesRowNum = mentionesRowNumQuery(queries);
//        rdd = usesRowNum ? enrichRddWithRowNum(rdd) : rdd;
//
//        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
//        Broadcast<Collection<Query>> queryBc = cxt.broadcast(queries);
//        return JavaRddOps.mapPartitions(rdd, bindingIt -> {
//            Collection<Query> qs = queryBc.getValue();
//            return quadMapper(qs).apply(bindingIt);
//        });
//    }

    public static StreamFunction<Binding, Binding> createMapperBindings(Op op) {
        return upstream -> {
            DatasetGraph ds = DatasetGraphFactory.create();
            Context cxt = ARQ.getContext().copy();
            ExecutionContext execCxt = new ExecutionContext(cxt, ds.getDefaultGraph(), ds, QC.getFactory(cxt));

            return upstream.flatMap(binding -> {
                QueryIterator r = QC.execute(op, binding, execCxt);
                Iter.onClose(r, r::close);
                return Streams.stream(r).onClose(r::close);
            });
        };
    }

    
    public static Function<Binding, Stream<Triple>> createMapperTriples(Template template) {
        return binding -> Streams.stream(TemplateLib.calcTriples(template.getTriples(), Collections.singleton(binding).iterator()));
    }

//    public static JavaRDD<Triple> tarqlTriplesRx(JavaRDD<Binding> rdd, Query query) {
//        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");
//
//        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
//        Broadcast<Query> queryBc = cxt.broadcast(query);
//        RxFunction<Binding, Triple> mapper = upstream -> {
//            Query q = queryBc.getValue();
//            Template template = q.getConstructTemplate();
//            Op op = Algebra.compile(q);
//            op = tarqlOptimize(op);
//
//            return upstream
//                    .compose(QueryFlowOps.createMapperBindings(op))
//                    .flatMap(QueryFlowOps.createMapperTriples(template)::apply);
//        };
//
//        rdd = enrichRddWithRowNumIfNeeded(rdd, query);
//        return JavaRddRxOps.mapPartitions(rdd, mapper);
//    }

    /** Apply default optimizations for algebra expressions meant for tarql
     * Combines EXTENDS */
    public static Op tarqlOptimize(Op op) {
        Op result = Transformer.transform(new TransformExtendCombine(), op);
        return result;
    }

    /**
     * Use an RDD of bindings as initial bindings for a construct query in order to yield quads.
     * This is conceptually the same approach as done by the tool 'tarql', hence the name.
     */
//    public static JavaRDD<Quad> tarqlQuads(JavaRDD<Binding> rdd, Query query) {
//        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");
//
//        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
//        Broadcast<Query> queryBc = cxt.broadcast(query);
//        RxFunction<Binding, Quad> mapper = upstream -> {
//            Query q = queryBc.getValue();
//            Template template = q.getConstructTemplate();
//            Op op = Algebra.compile(q);
//            op = tarqlOptimize(op);
//
//            return upstream
//                    .compose(QueryFlowOps.createMapperBindings(op))
//                    .flatMap(QueryFlowOps.createMapperQuads(template)::apply);
//        };
//
//        rdd = enrichRddWithRowNumIfNeeded(rdd, query);
//        return JavaRddRxOps.mapPartitions(rdd, mapper);
//    }
    
    /* Tarql mode: after applying all statements to the given dataset, that dataset is emitted */
//    public static StreamFunction<Binding, Dataset> mapperDatasets(List<SparqlStmt> stmts, boolean constructMode) {
//    	Function<Binding, Dataset> mapper = mapperDatasetsCore(stmts, constructMode);
//    	return upstream -> upstream.map(mapper);
//    }
    
    /** Each binding becomes its own dataset */
    public static JavaRDD<Dataset> tarqlDatasets(JavaRDD<Binding> rdd, Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
        Broadcast<Query> queryBc = cxt.broadcast(query);
        RxFunction<Binding, Dataset> mapper = upstream -> {
            Query q = queryBc.getValue();
            Template template = q.getConstructTemplate();
            Op op = Algebra.compile(q);
            op = tarqlOptimize(op);

            return upstream
                    .compose(QueryFlowOps.createMapperBindings(op))
                    .flatMap(QueryFlowOps.createMapperQuads(template)::apply)
                    .reduceWith(DatasetGraphFactory::create, (dsg, quad) -> { dsg.add(quad); return dsg; })
                    .map(DatasetFactory::wrap)
                    .toFlowable();
        };

        rdd = enrichRddWithRowNumIfNeeded(rdd, query);
        return JavaRddRxOps.mapPartitions(rdd, mapper);
    }

    public static boolean mentionesRowNum(SparqlStmt sparqlStmt) {
    	Set<Node> nodes = SparqlStmtUtils.mentionedNodes(sparqlStmt);
        boolean result = nodes.contains(ROWNUM);
        return result;
    }

    /*
    public static boolean mentionesRowNumQuery(Collection<Query> queries) {
    	Set<Var> nodes = queries.stream()
    			.map(QueryUtils::mentionedVars)
    			.flatMap(Collection::stream)
    			.collect(Collectors.toSet());
        boolean result = nodes.contains(ROWNUM);
        return result;
    }
    */

    public static boolean mentionesRowNum(Collection<SparqlStmt> sparqlStmts) {
    	boolean result = sparqlStmts.stream().anyMatch(JavaRddOfBindingsOps::mentionesRowNum);
        return result;
    }
    
    /** If the given query mentions a variable ?ROWNUM (upper case) then the input rdd of bindings is
     *  zipped with index */
    public static JavaRDD<Binding> enrichRddWithRowNumIfNeeded(JavaRDD<Binding> rdd, Query query) {
        Set<Var> mentionedVars = query.isConstructType()
                ? QuadPatternUtils.getVarsMentioned(query.getConstructTemplate().getQuads())
                : new HashSet<>();

        Op op = Algebra.compile(query);
        Collection<Var> patternVars = OpVars.mentionedVars(op);
        mentionedVars.addAll(patternVars);
        boolean usesRowNum = mentionedVars.contains(ROWNUM);

        JavaRDD<Binding> result = usesRowNum ? enrichRddWithRowNum(rdd) : rdd;

        return result;
    }
    
    public static JavaRDD<Binding> enrichRddWithRowNum(JavaRDD<Binding> rdd) {
        return rdd.zipWithIndex().map(bi -> BindingFactory.binding(bi._1, ROWNUM, NodeValue.makeInteger(bi._2 + 1).asNode()));
    }
}
