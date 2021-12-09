package net.sansa_stack.spark.io.rdf.output;

import net.sansa_stack.spark.rdd.function.JavaRddFunction;
import org.aksw.commons.lambda.serializable.SerializableBiConsumer;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.spark.api.java.JavaRDD;

import java.util.function.BiConsumer;

public class RddRdfDispatcherImpl<T>
    implements RddRdfDispatcher<T>
{
    protected BiConsumer<T, StreamRDF> sendRecordToStreamRDF;
    protected JavaRddFunction<T, Triple> convertToTriple;
    protected JavaRddFunction<T, Quad> convertToQuad;

    public RddRdfDispatcherImpl(
            BiConsumer<T, StreamRDF> sendRecordToStreamRDF,
            JavaRddFunction<T, Triple> convertToTriple,
            JavaRddFunction<T, Quad> convertToQuad) {
        super();
        this.sendRecordToStreamRDF = sendRecordToStreamRDF;
        this.convertToTriple = convertToTriple;
        this.convertToQuad = convertToQuad;
    }

    @Override
    public void sendRecordToStreamRDF(T record, StreamRDF streamRDF) {
        this.sendRecordToStreamRDF.accept(record, streamRDF);
    }

    @Override
    public JavaRDD<Triple> convertToTriple(JavaRDD<T> record) {
        return this.convertToTriple.apply(record);
    }

    public JavaRDD<Quad> convertToQuad(JavaRDD<T> record) {
        return this.convertToQuad(record);
    }

    /**
     * Create method.
     * Note that the 'sendRecordToSTreamRDF' parameter must be serializable
     * because it is used within mapPartitions.
     *
     * The convertToTriple and convertToQuad arguments are applied on the
     * driver while preparing the spark operations.
     *
     * @param sendRecordToStreamRDF
     * @param convertToTriple
     * @param convertToQuad
     * @param <T>
     * @return
     */
    public static <T> RddRdfDispatcherImpl create(
            SerializableBiConsumer<T, StreamRDF> sendRecordToStreamRDF,
            JavaRddFunction<T, Triple> convertToTriple,
            JavaRddFunction<T, Quad> convertToQuad) {

        return new RddRdfDispatcherImpl<>(
                sendRecordToStreamRDF, convertToTriple, convertToQuad);
    }

    public static RddRdfDispatcherImpl<Triple> createForTriple() {
        return RddRdfDispatcherImpl.<Triple>create(
                (triple, streamRDF) -> streamRDF.triple(triple),
                x -> x,
                x -> x.map(triple -> Quad.create(Quad.defaultGraphNodeGenerated, triple)));
    }

    public static RddRdfDispatcherImpl<Quad> createForQuad() {
        return RddRdfDispatcherImpl.<Quad>create(
                (quad, streamRDF) -> streamRDF.quad(quad),
                x -> x.map(Quad::asTriple),
                x -> x);
    }

    public static RddRdfDispatcherImpl<Graph> createForGraph() {
        return RddRdfDispatcherImpl.<Graph>create(
                (graph, streamRDF) -> StreamRDFOps.sendDatasetToStream(DatasetGraphFactory.wrap(graph), streamRDF),
                x -> x.flatMap(Graph::find),
                x -> x.flatMap(graph -> graph.find().mapWith(t -> new Quad(Quad.defaultGraphNodeGenerated, t)))
        );
    }

    public static RddRdfDispatcherImpl<DatasetGraph> createForDatasetGraph() {
        return RddRdfDispatcherImpl.<DatasetGraph>create(
                (dg, streamRDF) -> StreamRDFOps.sendDatasetToStream(dg, streamRDF),
                x -> x.flatMap(dg -> WrappedIterator.create(dg.find()).mapWith(Quad::asTriple)),
                x -> x.flatMap(DatasetGraph::find)
        );
    }

    public static RddRdfDispatcherImpl<Model> createForModel() {
        return RddRdfDispatcherImpl.<Model>create(
                (model, streamRDF) -> StreamRDFOps.sendDatasetToStream(DatasetGraphFactory.wrap(model.getGraph()), streamRDF),
                x -> x.flatMap(model -> model.getGraph().find()),
                x -> x.flatMap(model -> model.getGraph().find().mapWith(t -> new Quad(Quad.defaultGraphNodeGenerated, t)))
        );
    }

    public static RddRdfDispatcherImpl<Dataset> createForDataset() {
        return RddRdfDispatcherImpl.<Dataset>create(
                (ds, streamRDF) -> StreamRDFOps.sendDatasetToStream(ds.asDatasetGraph(), streamRDF),
                x -> x.flatMap(ds -> WrappedIterator.create(ds.asDatasetGraph().find()).mapWith(Quad::asTriple)),
                x -> x.flatMap(ds -> ds.asDatasetGraph().find())
        );
    }
}