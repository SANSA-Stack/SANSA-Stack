package net.sansa_stack.hadoop.format.jena.base;

import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import org.apache.jena.riot.Lang;

public class RecordReaderRdfConf
    extends RecordReaderConf
{
    protected String prefixesMaxLengthKey;
    protected Lang lang;

    public String getPrefixesMaxLengthKey() {
        return prefixesMaxLengthKey;
    }

    public Lang getLang() {
        return lang;
    }

    public RecordReaderRdfConf(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            CustomPattern recordSearchPattern,
            String prefixesMaxLengthKey,
            Lang lang) {
        super(minRecordLengthKey, maxRecordLengthKey, probeRecordCountKey, recordSearchPattern);
        this.prefixesMaxLengthKey = prefixesMaxLengthKey;
        this.lang = lang;
    }
}
