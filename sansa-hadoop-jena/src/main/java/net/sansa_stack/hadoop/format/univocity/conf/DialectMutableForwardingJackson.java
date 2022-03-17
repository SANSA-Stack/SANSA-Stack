package net.sansa_stack.hadoop.format.univocity.conf;

import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableForwarding;

public class DialectMutableForwardingJackson<T extends DialectMutable>
        extends DialectForwardingJackson<T>
        implements DialectMutableForwarding<T> {

    public DialectMutableForwardingJackson(T delegate) {
        super(delegate);
    }
}
