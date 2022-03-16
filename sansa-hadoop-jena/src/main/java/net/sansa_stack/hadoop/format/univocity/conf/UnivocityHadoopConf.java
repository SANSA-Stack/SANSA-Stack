package net.sansa_stack.hadoop.format.univocity.conf;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonMerge;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
public class UnivocityHadoopConf
{
    protected DialectMutableForwardingJackson<?> dialect;

    public UnivocityHadoopConf() {
        this(new DialectMutableImpl());
    }

    public UnivocityHadoopConf(DialectMutable dialectStore) {
        this.dialect = new DialectMutableForwardingJacksonString<>(dialectStore);
    }

    /** The csvw dialect roughly corresponds to univocity's CsvFormat class */
    @JsonMerge
    public DialectMutable getDialect() {
        return dialect;
    }
}