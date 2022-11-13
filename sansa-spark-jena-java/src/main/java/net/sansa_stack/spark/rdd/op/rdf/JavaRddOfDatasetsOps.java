package net.sansa_stack.spark.rdd.op.rdf;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.ext.com.google.common.collect.Streams;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;

import net.sansa_stack.spark.rdd.function.JavaRddFunction;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import scala.Tuple2;

public class JavaRddOfDatasetsOps {
	
    public static JavaRDD<Quad> flatMapToQuads(JavaRDD<? extends Dataset> rdd) {
    	// TODO Does the iterator close itself on end? If not then we should take care of it.
        return rdd.flatMap(ds -> ds.asDatasetGraph().find());
    }

    /** Maps a dataset to triples - emits quads from named graphs as triples by dropping the named graph */
    public static JavaRDD<Triple> flatMapToTriples(JavaRDD<? extends Dataset> rdd) {
    	// TODO Does the iterator close itself on end? If not then we should take care of it.
        return rdd.flatMap(ds -> Iter.iter(ds.asDatasetGraph().find()).map(Quad::asTriple));
    }

	
    public static JavaPairRDD<String, Model> flatMapToNamedModels(JavaRDD<? extends Dataset> rdd) {
        // TODO Add a flag to include the default graph under a certain name such as
        // Quad.defaultGraph
        return rdd.flatMapToPair(
                ds -> Streams.stream(ds.listNames())
                    .map(iri -> new Tuple2<>(iri, ds.getNamedModel(iri)))
                    .iterator());
    }

    /**
     * Group all graphs by their <b>named graph</b> IRIs. Effectively merges triples
     * from all named graphs with the same IRI. Removes duplicated triples.
     *
     * Ignores default graphs which get lost.
     */
    public static JavaRDD<DatasetOneNg> groupNamedGraphsByGraphIri(JavaRDD<? extends Dataset> rdd, boolean distinct,
                                                                   boolean sortGraphsByIri, int numPartitions) {

        // Note: Model is usually ModelCom so we get out-of-the-box serialization
        // If we used Graph we'd have to deal with a lot more variation in kryo
        JavaPairRDD<String, Model> step1 = flatMapToNamedModels(rdd);
        JavaPairRDD<String, Model> step2 = JavaRddOfNamedModelsOps.groupNamedModels(
                step1, distinct, sortGraphsByIri, numPartitions);

        JavaRDD<DatasetOneNg> result = JavaRddOfNamedModelsOps.mapToDatasets(step2);

        return result;
    }
    JavaRddFunction<Dataset, Quad> flatMapToQuadsViaConstruct(Query query)  {
        Objects.requireNonNull(query);

        return rdd -> {
            Broadcast<Query> queryBc = JavaSparkContextUtils.fromRdd(rdd).broadcast(query);

            return rdd.flatMap(in -> {
                Iterator<Quad> r;
                Query q = queryBc.value();

                try (QueryExecution qe = QueryExecutionFactory.create(q, in)) {
                    // Materialize
                    List<Quad> list = Lists.newArrayList(qe.execConstructQuads());
                    r = list.iterator();
                }
                return r;
            });
        };
    }


    /* ResourceInDatasetFlowOps is currently in jsa-rx-io; needs consolidation...
    public static JavaRddFunction<Dataset, NodesInDataset> mapToNodesInDataset(Query graphAndNodeSelector) {
        String queryStr = graphAndNodeSelector.toString();

        return rdd -> rdd.mapPartitions(it -> {
           Query query = QueryFactory.create(queryStr);
           return Streams.stream(it)
                   .map(ResourceInDatasetFlowOps.mapToGroupedResourceInDataset(query))
                   .iterator();
        });
    }
     */

}

