package net.sansa_stack.hadoop.format.univocity.conf;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.impl.DialectForwardingBase;

/** Annotated forwarding dialect implementation that tells
 * Jackson to only rely on this class' bean properties
 * (excluding getDelegate). */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties("delegate")
public class DialectForwardingJackson<T extends Dialect>
        extends DialectForwardingBase<T> {
    public DialectForwardingJackson(T delegate) {
        super(delegate);
    }
}
