package net.sansa_stack.hadoop.format.jena.base;

import net.sansa_stack.hadoop.core.Accumulating;
import net.sansa_stack.hadoop.core.RecordReaderGenericBase;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.shared.PrefixMapping;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Pattern;

public abstract class RecordReaderGenericRdfBase<U, G, A, T>
        extends RecordReaderGenericBase<U, G, A, T>
{
    protected final String baseIriKey;
    protected final String headerBytesKey;
    protected String prefixesMaxLengthKey;

    protected String baseIri;
    protected Lang lang;

    public RecordReaderGenericRdfBase(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            String prefixesMaxLengthKey,
            CustomPattern recordSearchPattern,
            Lang lang,
            Accumulating<U, G, A, T> accumulating) {
        super(minRecordLengthKey,
                maxRecordLengthKey,
                probeRecordCountKey,
                recordSearchPattern,
                // FileInputFormatRdfBase.BASE_IRI_KEY,
                // FileInputFormatRdfBase.PREFIXES_KEY,
                accumulating);
        this.lang = lang;
        this.prefixesMaxLengthKey = prefixesMaxLengthKey;
        this.baseIriKey = FileInputFormatRdfBase.BASE_IRI_KEY;
        this.headerBytesKey = FileInputFormatRdfBase.PREFIXES_KEY;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        super.initialize(inputSplit, context);

        Configuration job = context.getConfiguration();

        baseIri = job.get(baseIriKey);

        Model model = FileInputFormatRdfBase.getModel(job, headerBytesKey);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RDFDataMgr.write(baos, model, RDFFormat.TURTLE_PRETTY);
        // val prefixBytes = baos.toByteArray
        preambleBytes = baos.toByteArray();
    }

}
