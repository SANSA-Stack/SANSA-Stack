package net.sansa_stack.rdf.common.io.hadoop.rdf.trig;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.rdf.common.io.hadoop.rdf.base.RecordReaderGenericRdfBase;
import org.aksw.jena_sparql_api.rx.RDFDataMgrRx;
import org.apache.jena.query.Dataset;
import org.apache.jena.riot.Lang;

import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * RecordReader for the Trig RDF format that groups consecutive quads having the
 * same IRI for the graph component into Datasets.
 */
public class RecordReaderTrigDataset
    extends RecordReaderGenericRdfBase<Dataset>
{
    public static String MIN_RECORD_LENGTH_KEY = "mapreduce.input.trigrecordreader.record.minlength";
    public static String MAX_RECORD_LENGTH_KEY = "mapreduce.input.trigrecordreader.record.maxlength";
    public static String PROBE_RECORD_COUNT_KEY = "mapreduce.input.trigrecordreader.probe.count";

    protected static final Pattern trigFwdPattern = Pattern.compile("@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    public RecordReaderTrigDataset() {
        super(
                MIN_RECORD_LENGTH_KEY,
                MAX_RECORD_LENGTH_KEY,
                PROBE_RECORD_COUNT_KEY,
                trigFwdPattern,
                Lang.TRIG);
    }

    @Override
    protected Flowable<Dataset> parse(Callable<InputStream> inputStreamSupplier) {
        Flowable<Dataset> result = RDFDataMgrRx.createFlowableDatasets(inputStreamSupplier, lang, baseIri);
        return result;
    }
}
