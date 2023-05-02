package net.sansa_stack.spark.rdd.op.rdf;

import net.sansa_stack.spark.rdd.function.JavaRddFunction;
import org.aksw.jenax.dboe.dataset.impl.DatasetGraphQuadsImpl;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class JavaRddOfQuadsOps {

    /** group quads by graph IRI into a pairs (graphIri, Model) */
    public static JavaPairRDD<String, Model> groupByNamedGraph(
            JavaRDD<Quad> rdd) {
        return rdd
                .mapToPair(quad -> new Tuple2<>(quad.getGraph().getURI(), quad))
                .combineByKey(
                    triple -> ModelFactory.createDefaultModel(),
                    (model, quad) -> { model.getGraph().add(quad.asTriple()); return model; },
                    (m1, m2) -> { m1.add(m2); return m1; });
    }


    public static JavaRDD<Dataset> mapToDataset(JavaRDD<Quad> rdd) {
        return rdd.map(quad -> {
            DatasetGraph dg = DatasetGraphFactory.create();
            dg.add(quad);
            Dataset r = DatasetFactory.wrap(dg);
            return r;
        });
    }

    public static JavaRddFunction<Quad, Quad> mapIntoGraph(Node graphNode) {
        return rdd -> rdd.map(quad -> new Quad(graphNode, quad.asTriple()));
    }

    public static JavaRddFunction<Quad, Triple> mapToTriples(Node graphNode) {
        return rdd -> rdd.map(Quad::asTriple);
    }

    /**
     * Post process RDF data - sort, distinct, repartition
     * Sort quads by their string representation (relies on {@link NodeFmtLib#str}) */
    public static JavaRDD<Quad> postProcess(JavaRDD<Quad> rdd, boolean sort, boolean ascending, boolean distinct, int numPartitions) {

        if (distinct) {
            rdd = rdd.distinct();
        }

        if (sort) {
            rdd = rdd.sortBy(NodeFmtLib::str, ascending, numPartitions);
        }

        return rdd;
    }
}
