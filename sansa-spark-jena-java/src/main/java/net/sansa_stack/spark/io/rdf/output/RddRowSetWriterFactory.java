package net.sansa_stack.spark.io.rdf.output;

import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;

public class RddRowSetWriterFactory
    extends RddRowSetWriterSettings<RddRowSetWriterFactory>
{
    public static RddRowSetWriterFactory create() {
        return new RddRowSetWriterFactory();
    }

    public RddRowSetWriterFactory validate() {
        RddRowSetWriterUtils.requireValidLang(outputLang);
        return self();
    }

    public RddRowSetWriter forRowSet(JavaResultSetSpark rs) {
        return new RddRowSetWriter().configureFrom(this).setRowSet(rs);
    }
}
