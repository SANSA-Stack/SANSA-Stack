package net.sansa_stack.hadoop.jena.rdf.base;

import net.sansa_stack.hadoop.generic.Accumulating;
import net.sansa_stack.hadoop.generic.RecordReaderGenericBase;
import org.apache.jena.riot.Lang;

import java.util.regex.Pattern;

public abstract class RecordReaderGenericRdfBase<U, G, A, T>
    extends RecordReaderGenericBase<U, G, A, T>
{
    protected Lang lang;

    protected String prefixesMaxLengthKey;

    public RecordReaderGenericRdfBase(
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
                recordSearchPattern,
                FileInputFormatRdfBase.BASE_IRI_KEY,
                FileInputFormatRdfBase.PREFIXES_KEY,
                accumulating);
        this.lang = lang;
        this.prefixesMaxLengthKey = prefixesMaxLengthKey;
    }
}
