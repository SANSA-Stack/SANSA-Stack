package net.sansa_stack.spark.io.rdf.input.api;

import net.sansa_stack.spark.io.rdf.output.RddRdfOpsImpl;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

public class RdfSources {
    public static RdfSource ofTriples(JavaRDD<Triple> rdd) {
        return new RdfSourceFromRdd<>(rdd, RddRdfOpsImpl.createForTriple(), PrefixMapFactory.create());
    }

    public static RdfSource ofModels(JavaRDD<Model> rdd) {
        return new RdfSourceFromRdd<>(rdd, RddRdfOpsImpl.createForModel(), PrefixMapFactory.create());
    }

    public static RdfSource ofQuads(JavaRDD<Quad> rdd) {
        return new RdfSourceFromRdd<>(rdd, RddRdfOpsImpl.createForQuad(), PrefixMapFactory.create());
    }
    
    public static RdfSource ofDatasets(JavaRDD<DatasetOneNg> rdd) {
        return new RdfSourceFromRdd<>(rdd, RddRdfOpsImpl.createForDataset(), PrefixMapFactory.create());
    }
}
