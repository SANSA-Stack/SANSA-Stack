package net.sansa_stack.spark.rdd.op.rdf;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.Quad;
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
}
