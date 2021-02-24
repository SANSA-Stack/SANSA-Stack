package net.sansa_stack.rdf.common.io.hadoop.rdf.base;

import net.sansa_stack.rdf.common.io.hadoop.generic.RecordReaderGenericBase;
import net.sansa_stack.rdf.common.io.hadoop.rdf.base.FileInputFormatRdfBase;
import org.apache.jena.riot.Lang;

import java.util.regex.Pattern;

public abstract class RecordReaderGenericRdfBase<T>
    extends RecordReaderGenericBase<T>
{
    protected Lang lang;

    public RecordReaderGenericRdfBase(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            Pattern recordSearchPattern,
            Lang lang) {
        super(minRecordLengthKey,
                maxRecordLengthKey,
                probeRecordCountKey,
                recordSearchPattern,
                FileInputFormatRdfBase.BASE_IRI_KEY,
                FileInputFormatRdfBase.PREFIXES_KEY);
    }
}
