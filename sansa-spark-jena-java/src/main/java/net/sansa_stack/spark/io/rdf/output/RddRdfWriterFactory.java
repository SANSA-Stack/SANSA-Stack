package net.sansa_stack.spark.io.rdf.output;

import org.aksw.jenax.arq.dataset.api.DatasetGraphOneNg;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

/**
 * A factory for {@link RddRdfWriter} instances which enables validation of settings at an early stage
 * using {@link #validate()}.
 * Using {@link RddRdfWriter} directly may cause misconfigurations to only be detected at the near end of a long
 * running spark job, such as when attempting to write out the result of a long running sort operation.
 *
 */
public class RddRdfWriterFactory
    extends RddRdfWriterSettings<RddRdfWriterFactory>
{
    public static RddRdfWriterFactory create() {
        return new RddRdfWriterFactory();
    }

    @Override
    protected RddRdfWriterFactory self() {
        return super.self();
    }

    public RddRdfWriterFactory validate() {
        RddRdfWriter.validate(this);
        return self();
    }

    public RddRdfWriter<Triple> forTriple(JavaRDD<? extends Triple> rdd) {
        return RddRdfWriter.createForTriple().configureFrom(this).setRdd(rdd);
    }

    public RddRdfWriter<Quad> forQuad(JavaRDD<? extends Quad> rdd) {
        return RddRdfWriter.createForQuad().configureFrom(this).setRdd(rdd);
    }

    public RddRdfWriter<Graph> forGraph(JavaRDD<? extends Graph> rdd) {
        return RddRdfWriter.createForGraph().configureFrom(this).setRdd(rdd);
    }

    public RddRdfWriter<DatasetGraphOneNg> forDatasetGraph(JavaRDD<? extends DatasetGraphOneNg> rdd) {
        return RddRdfWriter.createForDatasetGraph().configureFrom(this).setRdd(rdd);
    }

    public RddRdfWriter<Model> forModel(JavaRDD<? extends Model> rdd) {
        return RddRdfWriter.createForModel().configureFrom(this).setRdd(rdd);
    }

    public RddRdfWriter<DatasetOneNg> forDataset(JavaRDD<? extends DatasetOneNg> rdd) {
        return RddRdfWriter.createForDataset().configureFrom(this).setRdd(rdd);
    }
}
