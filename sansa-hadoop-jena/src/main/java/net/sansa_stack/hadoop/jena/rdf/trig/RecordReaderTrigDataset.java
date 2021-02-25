package net.sansa_stack.hadoop.jena.rdf.trig;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.jena.rdf.base.RecordReaderGenericRdfBase;
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
    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.trig.dataset.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.trig.dataset.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.trig.dataset.record.probecount";
    public static final String PREFIXES_MAXLENGTH_KEY = "mapreduce.input.trig.dataset.prefixes.maxlength";


    protected static final Pattern trigFwdPattern = Pattern.compile("@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    public RecordReaderTrigDataset() {
        super(
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                PREFIXES_MAXLENGTH_KEY,
                trigFwdPattern,
                Lang.TRIG);
    }

    @Override
    protected Flowable<Dataset> parse(Callable<InputStream> inputStreamSupplier) {
        Flowable<Dataset> result = RDFDataMgrRx.createFlowableDatasets(inputStreamSupplier, lang, baseIri);
        return result;
    }
}
