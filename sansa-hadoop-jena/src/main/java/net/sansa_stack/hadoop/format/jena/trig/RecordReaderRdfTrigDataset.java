package net.sansa_stack.hadoop.format.jena.trig;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.core.Accumulating;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.core.pattern.CustomPatternJava;
import net.sansa_stack.hadoop.format.jena.base.RecordReaderGenericRdfAccumulatingBase;
import net.sansa_stack.hadoop.format.jena.base.RecordReaderRdfConf;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.arq.dataset.impl.DatasetOneNgImpl;
import org.aksw.jenax.sparql.query.rx.RDFDataMgrRx;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * RecordReader for the Trig RDF format that groups consecutive quads having the
 * same IRI for the graph component into Datasets.
 */
public class RecordReaderRdfTrigDataset
        extends RecordReaderGenericRdfAccumulatingBase<Quad, Node, DatasetOneNg, DatasetOneNg> {
    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.trig.dataset.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.trig.dataset.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.trig.dataset.record.probecount";

    public static final String ELEMENT_PROBECOUNT_KEY = "mapreduce.input.trig.dataset.element.probecount";

    public static final String PREFIXES_MAXLENGTH_KEY = "mapreduce.input.trig.dataset.prefixes.maxlength";


    /**
     * This is the pattern for trig data where graphs are separated by '{'
     */
    protected static final CustomPattern trigFwdPatternWorking = CustomPatternJava
            .compile("@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{",
                    Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    protected static final CustomPattern trigFwdPattern = CustomPatternJava
            .compile(".",
                    Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);


    public static class AccumulatingDataset
            implements Accumulating<Quad, Node, DatasetOneNg, DatasetOneNg> {

        @Override
        public Node classify(Quad item) {
            return item.getGraph();
        }

        @Override
        public DatasetOneNg createAccumulator(Node groupKey) {
            // Is null handling needed here?
            String graphName = groupKey == null
                    ? Quad.defaultGraphNodeGenerated.getURI()
                    : groupKey.getURI();

            // TODO Allow for a custom graph factory here
            // Graph graph = GraphFactory.createDefaultGraph();
            DatasetOneNg result = DatasetOneNgImpl.create(graphName);
            result.begin(ReadWrite.WRITE);
            return result;
            // return DatasetFactoryEx.createInsertOrderPreservingDataset();
        }

        @Override
        public void accumulate(DatasetOneNg accumulator, Quad item) {
            accumulator.asDatasetGraph().add(item);
        }

        @Override
        public DatasetOneNg accumulatedValue(DatasetOneNg accumulator) {
            accumulator.commit();
            return accumulator;
        }
    }

    public RecordReaderRdfTrigDataset() {
        super(new RecordReaderRdfConf(
                        RECORD_MINLENGTH_KEY,
                        RECORD_MAXLENGTH_KEY,
                        RECORD_PROBECOUNT_KEY,
                        trigFwdPattern,
                        PREFIXES_MAXLENGTH_KEY,
                        Lang.TRIG),
                new AccumulatingDataset());
    }

    @Override
    protected Stream<Quad> parse(InputStream in, boolean isProbe) {
        Stream<Quad> result = setupParser(in, isProbe).streamQuads();

        // List<Quad> tmp = result.collect(Collectors.toList());
        // System.out.println("Got " + tmp.size() + " items");
        // result = tmp.stream();

        // Stream<Quad> result = RDFDataMgrRx.createFlowableQuads(() -> in, lang, baseIri).blockingStream();
        // System.out.println("isParallel: " + result.isParallel());
        // Flowable<Dataset> result = RDFDataMgrRx.createFlowableDatasets(inputStreamSupplier, lang, baseIri);
        return result;
    }

    // @Override
    protected Flowable<Quad> parse(Callable<InputStream> inputStreamSupplier) {
        Flowable<Quad> result = RDFDataMgrRx.createFlowableQuads(inputStreamSupplier, lang, baseIri);
        // Flowable<Dataset> result = RDFDataMgrRx.createFlowableDatasets(inputStreamSupplier, lang, baseIri);
        return result;
    }
}
