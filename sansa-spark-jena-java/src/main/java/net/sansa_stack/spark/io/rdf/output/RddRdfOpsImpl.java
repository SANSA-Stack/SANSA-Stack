package net.sansa_stack.spark.io.rdf.output;

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.aksw.commons.lambda.serializable.SerializableBiConsumer;
import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.aksw.jenax.arq.dataset.api.DatasetGraphOneNg;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.arq.util.quad.DatasetGraphUtils;
import org.aksw.jenax.arq.util.quad.DatasetUtils;
import org.aksw.jenax.arq.util.quad.QuadUtils;
import org.aksw.jenax.arq.util.triple.GraphUtils;
import org.aksw.jenax.arq.util.triple.ModelUtils;
import org.aksw.jenax.arq.util.triple.TripleUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.spark.api.java.JavaRDD;

import net.sansa_stack.spark.rdd.function.JavaRddFunction;

public class RddRdfOpsImpl<T>
    implements RddRdfOps<T>
{
    protected int componentCount;
    protected BiConsumer<T, StreamRDF> sendRecordToStreamRDF;
    protected JavaRddFunction<T, Triple> convertToTriple;
    protected JavaRddFunction<T, Quad> convertToQuad;
    protected JavaRddFunction<T, Node> convertToNode;
    protected Function<? super T, Comparable<?>> keyFunction;

    public RddRdfOpsImpl(
            int componentCount,
            BiConsumer<T, StreamRDF> sendRecordToStreamRDF,
            JavaRddFunction<T, Triple> convertToTriple,
            JavaRddFunction<T, Quad> convertToQuad,
            JavaRddFunction<T, Node> convertToNode,
            Function<? super T, Comparable<?>> keyFunction
            ) {
        super();
        this.componentCount = componentCount;
        this.sendRecordToStreamRDF = sendRecordToStreamRDF;
        this.convertToTriple = convertToTriple;
        this.convertToQuad = convertToQuad;
        this.convertToNode = convertToNode;
        this.keyFunction = keyFunction;
    }

    @Override
    public int getComponentCount() {
        return componentCount;
    }

    @Override
    public void sendRecordToStreamRDF(T record, StreamRDF streamRDF) {
        this.sendRecordToStreamRDF.accept(record, streamRDF);
    }

    @Override
    public JavaRDD<Triple> convertToTriple(JavaRDD<T> rdd) {
        return this.convertToTriple.apply(rdd);
    }

    @Override
    public JavaRDD<Quad> convertToQuad(JavaRDD<T> rdd) {
        return this.convertToQuad.apply(rdd);
    }

    @Override
    public JavaRDD<Node> convertToNode(JavaRDD<T> rdd) {
        return this.convertToNode.apply(rdd);
    }

    @Override
    public Function<? super T, Comparable<?>> getKeyFunction() {
        return keyFunction;
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
    public static <T> RddRdfOpsImpl<T> create(
            int componentCount,
            SerializableBiConsumer<T, StreamRDF> sendRecordToStreamRDF,
            JavaRddFunction<T, Triple> convertToTriple,
            JavaRddFunction<T, Quad> convertToQuad,
            JavaRddFunction<T, Node> convertToNode,
            SerializableFunction<? super T, Comparable<?>> keyFunction
    ) {

        return new RddRdfOpsImpl<>(componentCount,
                sendRecordToStreamRDF, convertToTriple, convertToQuad, convertToNode, keyFunction);
    }

    public static RddRdfOpsImpl<Triple> createForTriple() {
        return RddRdfOpsImpl.<Triple>create(
                3,
                (triple, streamRDF) -> streamRDF.triple(triple),
                x -> x,
                x -> x.map(triple -> Quad.create(Quad.defaultGraphNodeGenerated, triple)),
                x -> x.flatMap(triple -> TripleUtils.tripleToList(triple).iterator()),
                NodeFmtLib::str);
    }

    public static RddRdfOpsImpl<Quad> createForQuad() {
        return RddRdfOpsImpl.<Quad>create(
                4,
                (quad, streamRDF) -> streamRDF.quad(quad),
                x -> x.map(Quad::asTriple),
                x -> x,
                x -> x.flatMap(q -> QuadUtils.quadToList(q).iterator()),
                NodeFmtLib::str);
    }

    public static RddRdfOpsImpl<Graph> createForGraph() {
        return RddRdfOpsImpl.<Graph>create(
                3,
                (graph, streamRDF) -> StreamRDFOps.sendDatasetToStream(DatasetGraphFactory.wrap(graph), streamRDF),
                x -> x.flatMap(Graph::find),
                x -> x.flatMap(graph -> graph.find().mapWith(t -> new Quad(Quad.defaultGraphNodeGenerated, t))),
                x -> x.flatMap(GraphUtils::iterateNodes),
                x -> { throw new UnsupportedOperationException("Graphs don't have a sort key"); }
        );
    }

    public static RddRdfOpsImpl<DatasetGraphOneNg> createForDatasetGraph() {
        return RddRdfOpsImpl.<DatasetGraphOneNg>create(
                4,
                (dg, streamRDF) -> StreamRDFOps.sendDatasetToStream(dg, streamRDF),
                x -> x.flatMap(dg -> WrappedIterator.create(dg.find()).mapWith(Quad::asTriple)),
                x -> x.flatMap(DatasetGraph::find),
                x -> x.flatMap(DatasetGraphUtils::iterateNodes),
                x -> x.getGraphNode().toString() // pmap: false
        );
    }

    public static RddRdfOpsImpl<Model> createForModel() {
        return RddRdfOpsImpl.<Model>create(
                3,
                (model, streamRDF) -> StreamRDFOps.sendDatasetToStream(DatasetGraphFactory.wrap(model.getGraph()), streamRDF),
                x -> x.flatMap(model -> model.getGraph().find()),
                x -> x.flatMap(model -> model.getGraph().find().mapWith(t -> new Quad(Quad.defaultGraphNodeGenerated, t))),
                x -> x.flatMap(ModelUtils::iterateNodes),
                x -> { throw new UnsupportedOperationException("Models don't have a sort key"); }
        );
    }

    public static RddRdfOpsImpl<DatasetOneNg> createForDataset() {
        return RddRdfOpsImpl.<DatasetOneNg>create(
                4,
                (ds, streamRDF) -> StreamRDFOps.sendDatasetToStream(ds.asDatasetGraph(), streamRDF),
                x -> x.flatMap(ds -> WrappedIterator.create(ds.asDatasetGraph().find()).mapWith(Quad::asTriple)),
                x -> x.flatMap(ds -> ds.asDatasetGraph().find()),
                x -> x.flatMap(DatasetUtils::iterateNodes),
                x -> x.getGraphName()
        );
    }
}