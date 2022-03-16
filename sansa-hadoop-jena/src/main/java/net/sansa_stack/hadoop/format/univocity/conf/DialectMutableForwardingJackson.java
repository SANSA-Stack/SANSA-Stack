package net.sansa_stack.hadoop.format.univocity.conf;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableForwardingBase;

/** Annotated forwarding dialect implementation that tells
 * Jackson to only rely on this class' bean properties
 * (excluding getDelegate). */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties("delegate")
public class DialectMutableForwardingJackson<T extends DialectMutable>
        extends DialectMutableForwardingBase<T> {

    public DialectMutableForwardingJackson(T delegate) {
        super(delegate);
    }
}
