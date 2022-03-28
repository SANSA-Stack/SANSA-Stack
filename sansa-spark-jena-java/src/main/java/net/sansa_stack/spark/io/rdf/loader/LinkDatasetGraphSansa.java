package net.sansa_stack.spark.io.rdf.loader;

import com.google.common.util.concurrent.MoreExecutors;
import org.aksw.commons.lambda.serializable.SerializableSupplier;
import org.aksw.jenax.arq.connection.TransactionalDelegate;
import org.aksw.jenax.arq.util.streamrdf.StreamRDFToUpdateRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.rdflink.LinkDatasetGraph;
import org.apache.jena.rdflink.LinkSparqlUpdate;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Transactional;


/**
 * A {@link LinkDatasetGraph} implementation that loads files via the sansa
 * parser and sends the data (triples and quads) to a sink.
 * The sink typically batches the data and sends them as sparql update requests
 * to a {@link LinkSparqlUpdate}.
 *
 * @author raven
 *
 */
public class LinkDatasetGraphSansa
    implements LinkDatasetGraph, TransactionalDelegate
{
    // protected RdfSourceFactory sourceFactory;
    // protected FileSystem fileSystem;
    protected Configuration conf;
    protected SerializableSupplier<StreamRDF> sinkFactory;
    protected Transactional delegate;

    public LinkDatasetGraphSansa(Configuration conf, SerializableSupplier<StreamRDF> sinkFactory, Transactional delegate) {
        // this.fileSystem = fileSystem;
        this.conf = conf;
        this.sinkFactory = sinkFactory;
        this.delegate = delegate;
    }

    public static LinkDatasetGraphSansa create(Configuration conf, SerializableSupplier<LinkSparqlUpdate> link) {
        SerializableSupplier<StreamRDF> sinkFactory = () -> StreamRDFToUpdateRequest.createWithTrie(100, MoreExecutors.newDirectExecutorService(), updateRequest -> {
           LinkSparqlUpdate update = link.get();
           try {
               // System.out.println("Update: " + Thread.currentThread() + " " + updateRequest.toString().length());
               update.update(updateRequest);
           } finally {
               update.close();
           }
        });

        return new LinkDatasetGraphSansa(conf, sinkFactory, null);
    }

    @Override
    public void load(Node node, String s) {
        // RdfSource source = sourceFactory.get(s);
        // RDFLanguages.isTriples()
        throw new UnsupportedOperationException();
    }

    @Override
    public void load(String s) {
        try {
            StreamRDF sink = sinkFactory.get();
            Path path = new Path(s);
            AsyncRdfParserHadoop.parse(path, conf, sink);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        /*
        RdfSource source = sourceFactory.get(s);
        RddRdfWriter.sendToStreamRDF(
                source.asTriples().toJavaRDD(),
                (triple, streamRdf) -> streamRdf.triple(triple),
                streamRdfSupplier);
         */
    }

    @Override
    public void load(Node node, Graph graph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void load(Graph graph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(Node node, String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(Node node, Graph graph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(Graph graph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(Node node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadDataset(String s) {

    }

    @Override
    public void loadDataset(DatasetGraph datasetGraph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putDataset(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putDataset(DatasetGraph datasetGraph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearDataset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Graph get(Node node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Graph get() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DatasetGraph getDataset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
//        return Optional.ofNullable(getDelegate()).map(RDFLink::isClosed)
//                .orElse(false);
        return false; // FIXME Return the proper value
    }

    @Override
    public void close() {
//        Optional.ofNullable(getDelegate()).ifPresent(RDFLink::close);
    }

    @Override
    public Transactional getDelegate() {
        return delegate;
    }
}
