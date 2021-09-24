package net.sansa_stack.hadoop.jena.rdf.base;

import net.sansa_stack.hadoop.generic.Accumulating;
import net.sansa_stack.hadoop.generic.RecordReaderGenericBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Pattern;

public abstract class RecordReaderGenericRdfAccumulatingBase<U, G, A, T>
    extends RecordReaderGenericRdfBase<U, G, A, T>
{

    public RecordReaderGenericRdfAccumulatingBase(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            String prefixesMaxLengthKey,
            Pattern recordSearchPattern,
            Lang lang,
            Accumulating<U, G, A, T> accumulating) {
        super(minRecordLengthKey,
                maxRecordLengthKey,
                probeRecordCountKey,
                prefixesMaxLengthKey,
                recordSearchPattern,
                lang,
                // FileInputFormatRdfBase.BASE_IRI_KEY,
                // FileInputFormatRdfBase.PREFIXES_KEY,
                accumulating);
    }
}
