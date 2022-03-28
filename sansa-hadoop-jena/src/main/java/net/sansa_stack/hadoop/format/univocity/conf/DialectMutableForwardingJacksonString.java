package net.sansa_stack.hadoop.format.univocity.conf;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;

/** Wrapper that causes every attribute of Dialect to be serialized as string */
public class DialectMutableForwardingJacksonString<T extends DialectMutable>
        extends DialectMutableForwardingJackson<T> {

    public DialectMutableForwardingJacksonString(T delegate){
        super(delegate);
    }

    @JsonSerialize(using = ToStringSerializer.class) @Override
    public Boolean getHeader() { return super.getHeader(); }

    @JsonSerialize(using = ToStringSerializer.class) @Override
    public Boolean getSkipInitialSpace() { return super.getSkipInitialSpace(); }

    @JsonSerialize(using = ToStringSerializer.class) @Override
    public Boolean getSkipBlankRows() { return super.getSkipBlankRows(); }

    @JsonSerialize(using = ToStringSerializer.class) @Override
    public Long getHeaderRowCount() { return super.getHeaderRowCount(); }

    @JsonSerialize(using = ToStringSerializer.class) @Override
    public Long getSkipColumns() { return super.getSkipColumns(); }

    @JsonSerialize(using = ToStringSerializer.class)  @Override
    public Long getSkipRows() { return super.getSkipRows(); }
}
