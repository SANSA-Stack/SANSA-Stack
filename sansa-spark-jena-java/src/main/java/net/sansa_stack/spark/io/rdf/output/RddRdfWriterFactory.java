package net.sansa_stack.spark.io.rdf.output;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;

public class RddRdfWriterFactory
    extends RddRdfWriterSpettings<RddRdfWriterFactory>
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

    public RddRdfWriter<DatasetGraph> forDatasetGraph(JavaRDD<? extends DatasetGraph> rdd) {
        return RddRdfWriter.createForDatasetGraph().configureFrom(this).setRdd(rdd);
    }

    public RddRdfWriter<Model> forModel(JavaRDD<? extends Model> rdd) {
        return RddRdfWriter.createForModel().configureFrom(this).setRdd(rdd);
    }

    public RddRdfWriter<Dataset> forDataset(JavaRDD<? extends Dataset> rdd) {
        return RddRdfWriter.createForDataset().configureFrom(this).setRdd(rdd);
    }
}
