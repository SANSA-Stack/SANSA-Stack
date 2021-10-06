package net.sansa_stack.rdf.spark.rdd.op;

import org.apache.jena.ext.com.google.common.collect.Streams;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class JavaRddOfDatasetsOps {
    public static JavaPairRDD<String, Model> toNamedModels(JavaRDD<? extends Dataset> rdd) {
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
    public static JavaRDD<Dataset> groupNamedGraphsByGraphIri(JavaRDD<? extends Dataset> rdd, boolean distinct,
            boolean sortGraphsByIri, int numPartitions) {

        // Note: Model is usually ModelCom so we get out-of-the-box serialization
        // If we used Graph we'd have to deal with a lot more variation in kryo
        JavaPairRDD<String, Model> step1 = toNamedModels(rdd);
        JavaPairRDD<String, Model> step2 = JavaRddOfNamedModelsOps.groupNamedModels(
                step1, distinct, sortGraphsByIri, numPartitions);

        JavaRDD<Dataset> result = JavaRddOfNamedModelsOps.mapToDataset(step2);

        return result;
    }

}

