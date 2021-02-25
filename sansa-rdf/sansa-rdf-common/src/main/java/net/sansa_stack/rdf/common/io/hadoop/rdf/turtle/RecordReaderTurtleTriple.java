package net.sansa_stack.rdf.common.io.hadoop.rdf.turtle;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.rdf.common.io.hadoop.rdf.base.RecordReaderGenericRdfBase;
import org.aksw.jena_sparql_api.rx.RDFDataMgrRx;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

public class RecordReaderTurtleTriple
        extends RecordReaderGenericRdfBase<Triple>
{
    public static String MIN_RECORD_LENGTH_KEY = "mapreduce.input.trigrecordreader.record.minlength";
    public static String MAX_RECORD_LENGTH_KEY = "mapreduce.input.trigrecordreader.record.maxlength";
    public static String PROBE_RECORD_COUNT_KEY = "mapreduce.input.trigrecordreader.probe.count";

    protected static final Pattern turtlePattern = Pattern.compile(".", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE); //"@?base|@?prefix|(graph\\s*)?(<[^>]*>|_?:[^-\\s]+)\\s*\\{", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    public RecordReaderTurtleTriple() {
        super(
                MIN_RECORD_LENGTH_KEY,
                MAX_RECORD_LENGTH_KEY,
                PROBE_RECORD_COUNT_KEY,
                turtlePattern,
                Lang.TURTLE);
    }

    @Override
    protected Flowable<Triple> parse(Callable<InputStream> inputStreamSupplier) {

        boolean showData = false;

        if (showData) {
            String str = null;
            try {
                str = IOUtils.toString(inputStreamSupplier.call(), StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println("Provided Data: ------------------------------------------");
            System.out.println(str);

            byte[] bytes = str.getBytes();
            inputStreamSupplier = () -> new ByteArrayInputStream(bytes);
        }

        Flowable<Triple> result = RDFDataMgrRx.createFlowableTriples(inputStreamSupplier, lang, baseIri);
        return result;
    }
}
