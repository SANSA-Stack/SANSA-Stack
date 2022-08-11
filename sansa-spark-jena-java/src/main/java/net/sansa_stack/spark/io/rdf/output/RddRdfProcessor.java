package net.sansa_stack.spark.io.rdf.output;

import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.rdd.RDD;

public interface RddRdfProcessor {
    void triples(RDD<Triple> rdd);
    void quads(RDD<Quad> rdd);

    void models(RDD<Model> rdd);
    void datasets(RDD<DatasetOneNg> rdd);
}
