package net.sansa_stack.hadoop.format.univocity.conf;

import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.impl.DialectForwardingBase;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Annotated forwarding dialect implementation that tells
 * Jackson to only rely on this class' bean properties
 * (excluding getDelegate). */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties({"delegate", "lineTerminatorList"})
public class DialectForwardingJackson<T extends Dialect>
        extends DialectForwardingBase<T> {
    public DialectForwardingJackson(T delegate) {
        super(delegate);
    }
}
