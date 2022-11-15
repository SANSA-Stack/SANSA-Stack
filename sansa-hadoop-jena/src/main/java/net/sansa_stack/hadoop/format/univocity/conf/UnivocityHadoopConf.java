package net.sansa_stack.hadoop.format.univocity.conf;

import java.util.List;

import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonMerge;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
public class UnivocityHadoopConf
{
    protected DialectMutableForwardingJackson<?> dialect;

    protected boolean isTabs = false;
    // protected List<String> columnNamingSchemes;

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

    public boolean isTabs() {
        return isTabs;
    }

    public void setTabs(boolean tabs) {
        isTabs = tabs;
    }

//    public void setColumnNamingSchemes(List<String> columnNamingSchemes) {
//        this.columnNamingSchemes = columnNamingSchemes;
//    }
//
//    public List<String> getColumnNamingSchemes() {
//        return columnNamingSchemes;
//    }
}