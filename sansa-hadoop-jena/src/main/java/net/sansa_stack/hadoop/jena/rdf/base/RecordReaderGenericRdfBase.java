package net.sansa_stack.hadoop.jena.rdf.base;

import net.sansa_stack.hadoop.generic.RecordReaderGenericBase;
import org.apache.jena.riot.Lang;

import java.util.regex.Pattern;

public abstract class RecordReaderGenericRdfBase<T>
    extends RecordReaderGenericBase<T>
{
    protected Lang lang;

    protected String prefixesMaxLengthKey;

    public RecordReaderGenericRdfBase(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            String prefixesMaxLengthKey,
            Pattern recordSearchPattern,
            Lang lang) {
        super(minRecordLengthKey,
                maxRecordLengthKey,
                probeRecordCountKey,
                recordSearchPattern,
                FileInputFormatRdfBase.BASE_IRI_KEY,
                FileInputFormatRdfBase.PREFIXES_KEY);
        this.lang = lang;
        this.prefixesMaxLengthKey = prefixesMaxLengthKey;
    }
}
