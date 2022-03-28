package net.sansa_stack.spark.io.rdf.input.api;

import net.sansa_stack.spark.io.rdf.output.RddRdfOps;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

public class RdfSourceFromRdd<T>
    implements RdfSource
{
    protected JavaRDD<T> rdd;
    protected RddRdfOps<T> dispatcher;
    protected Model declaredPrefixes;

    public RdfSourceFromRdd(JavaRDD<T> rdd, RddRdfOps<T> dispatcher, Model declaredPrefixes) {
        this.rdd = rdd;
        this.dispatcher = dispatcher;
        this.declaredPrefixes = declaredPrefixes;
    }

    @Override
    public boolean usesQuads() {
        return dispatcher.usesQuads();
    }

    @Override
    public Model peekDeclaredPrefixes() {
        return declaredPrefixes;
    }

    @Override
    public RDD<Triple> asTriples() {
        return dispatcher.convertToTriple(rdd).rdd();
    }

    @Override
    public RDD<Quad> asQuads() {
        return dispatcher.convertToQuad(rdd).rdd();
    }

    @Override
    public RDD<Model> asModels() {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public RDD<DatasetOneNg> asDatasets() {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
